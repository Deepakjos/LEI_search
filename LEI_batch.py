import streamlit as st
import requests
import pandas as pd
import time
from typing import List, Dict, Any, Union

# --- Constants ---
BASE_URL = "https://api.gleif.org/api/v1/lei-records"
MAX_BATCH_SIZE = 200
REQUESTS_PER_MINUTE_LIMIT = 60
DEFAULT_TIMEOUT = 20 # seconds

# --- Utility Functions ---

def safe_get(data: Dict[str, Any], path: str, default: Any = "N/A") -> Any:
    """
    Safely retrieve a nested value from a dictionary using a dot-separated path.
    Example: safe_get(record, 'attributes.entity.legalName.name')
    """
    keys = path.split('.')
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return default
    return data if data is not None else default

def format_address(addr: Dict[str, Any]) -> str:
    """Formats an address dictionary into a single comma-separated string."""
    if not isinstance(addr, dict):
        return "N/A"
    
    parts = [
        ", ".join(safe_get(addr, 'addressLines', [])),
        safe_get(addr, 'city', ''),
        safe_get(addr, 'region', ''),
        safe_get(addr, 'postalCode', ''),
        safe_get(addr, 'country', '')
    ]
    return ", ".join(filter(None, parts)) or "N/A"

def parse_api_record(record: Dict[str, Any], search_query: str = None) -> Dict[str, Any]:
    """
    Normalizes a single LEI record from the API into a flat dictionary for display.
    """
    # Using safe_get for robust and readable data extraction
    base_data = {
        "LEI": safe_get(record, 'id'),
        "Legal Name": safe_get(record, 'attributes.entity.legalName.name'),
        "Entity Status": safe_get(record, 'attributes.entity.status'),
        "Legal Address": format_address(safe_get(record, 'attributes.entity.legalAddress', {})),
        "Registration Status": safe_get(record, 'attributes.registration.status'),
        "Managing LOU": safe_get(record, 'attributes.registration.managingLou'),
        "LEI Next Renewal Date": safe_get(record, 'attributes.registration.nextRenewalDate'),
        "Entity Expiration Date": safe_get(record, 'attributes.entity.entityExpirationDate'),
        "Registered At (Register)": safe_get(record, 'attributes.entity.registeredAt.id'),
        "Registered As": safe_get(record, 'attributes.entity.registeredAs'),
        "Validation Authority Entity ID (primary)": safe_get(record, 'attributes.registration.validationAuthority.validationAuthorityEntityID'),
        "Registration.OtherValidationAuthorities.OtherValidationAuthority.1.ValidationAuthorityEntityID": 
            safe_get(record, 'attributes.registration.otherValidationAuthorities.1.validationAuthorityEntityID')
    }
    
    if search_query:
        return {"Search Query": search_query, **base_data}
    return base_data


# --- API Client Class ---

class GleifClient:
    """A client to interact with the GLEIF API."""
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/vnd.api+json"})

    def _make_request(self, params: Dict[str, Any], url: str = BASE_URL) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Generic method to make a GET request and handle responses."""
        try:
            response = self.session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            return response.json().get("data", [])
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                st.warning("No records found for the given query.")
            else:
                st.error(f"API Error: {e.response.status_code} - {e.response.text}")
        except requests.exceptions.RequestException as e:
            st.error(f"Request failed: {e}")
        return []

    def fetch_by_ids(self, lei_ids: List[str]) -> List[Dict[str, Any]]:
        """Fetch multiple LEI records by a list of IDs."""
        params = {
            "filter[lei]": ",".join(lei_ids),
            "page[size]": len(lei_ids)
        }
        return self._make_request(params)

    def search_by_names(self, names: List[str]) -> List[Dict[str, Any]]:
        """Search for LEI records by a list of legal names, one by one."""
        all_results = []
        seen_leis = set()
        status_text = st.empty()

        for i, name in enumerate(names, 1):
            status_text.info(f"Processing query {i}/{len(names)}: '{name}'...")
            params = {
                "filter[entity.legalName]": name.strip(),
                "page[size]": 10 # Limit results per name to 10
            }
            results = self._make_request(params)
            for rec in results:
                parsed_rec = parse_api_record(rec, search_query=name)
                if parsed_rec["LEI"] not in seen_leis:
                    all_results.append(parsed_rec)
                    seen_leis.add(parsed_rec["LEI"])

            if len(names) > REQUESTS_PER_MINUTE_LIMIT and i < len(names):
                time.sleep(1)

        status_text.empty()
        return all_results

# --- UI Rendering Functions ---

def render_results(results: List[Dict[str, Any]], file_name: str):
    """Displays results in a dataframe and provides a download button."""
    if not results:
        st.warning("The search yielded no results.")
        return
        
    st.success(f"Found {len(results)} unique records.")
    df_results = pd.DataFrame(results)
    
    st.download_button(
        label="Download Results as CSV",
        data=df_results.to_csv(index=False).encode("utf-8"),
        file_name=file_name,
        mime="text/csv",
    )
    st.dataframe(df_results, use_container_width=True, hide_index=True)

def render_lei_tab(client: GleifClient):
    """Renders the UI for searching by LEI ID."""
    st.header("Batch Search by LEI ID")
    st.markdown("Upload a CSV file where the first column contains the 20-character LEI IDs.")

    uploaded_file = st.file_uploader("Upload LEI CSV", type=["csv"], key="lei_uploader")
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file, dtype=str)
            lei_list = [lei.strip().upper() for lei in df.iloc[:, 0].dropna() if len(lei.strip()) == 20]
            
            st.info(f"Found {len(lei_list)} valid LEI IDs in the uploaded file.")

            if st.button("Start LEI Batch Search", use_container_width=True):
                if not lei_list:
                    st.error("No valid 20-character LEI IDs to search.")
                else:
                    with st.spinner("Fetching data from GLEIF API..."):
                        all_results = []
                        # Process in batches
                        for i in range(0, len(lei_list), MAX_BATCH_SIZE):
                            batch_ids = lei_list[i:i + MAX_BATCH_SIZE]
                            batch_results = client.fetch_by_ids(batch_ids)
                            all_results.extend([parse_api_record(rec) for rec in batch_results])
                            if len(lei_list) > MAX_BATCH_SIZE and (i + MAX_BATCH_SIZE) < len(lei_list):
                                time.sleep(1) # Rate limit between batches
                        render_results(all_results, "lei_batch_results.csv")

        except Exception as e:
            st.error(f"Failed to process CSV file. Ensure it's a valid CSV. Error: {e}")


def render_name_tab(client: GleifClient):
    """Renders the UI for searching by Legal Name."""
    st.header("Batch Search by Legal Name")
    st.markdown("Upload a CSV file where the first column contains the legal names.")

    uploaded_file = st.file_uploader("Upload Legal Name CSV", type=["csv"], key="name_uploader")
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file, dtype=str)
            name_list = [name.strip() for name in df.iloc[:, 0].dropna() if len(name.strip()) >= 3]

            st.info(f"Found {len(name_list)} valid names (min. 3 chars) in the file.")
            
            if st.button("Start Name Batch Search", use_container_width=True):
                if not name_list:
                    st.error("No valid names to search.")
                else:
                    with st.spinner("Searching GLEIF database by name... This may take a while."):
                        results = client.search_by_names(name_list)
                        render_results(results, "name_batch_results.csv")
        except Exception as e:
            st.error(f"Failed to process CSV file. Ensure it's a valid CSV. Error: {e}")


# --- Main Application ---
def main():
    st.set_page_config(
        page_title="GLEIF LEI Search Tool",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

    st.title("🏛️ Global LEI Index Search (Batch Enabled)")
    st.markdown(
        "Use the tabs below to search for Legal Entity Identifiers (LEIs) via the official GLEIF API. "
        "Batch searches use a 1-second delay between requests for large lists to respect API rate limits."
    )
    
    # Instantiate the API client once
    client = GleifClient()

    tab1, tab2 = st.tabs(["Search by LEI ID", "Search by Legal Name"])

    with tab1:
        render_lei_tab(client)

    with tab2:
        render_name_tab(client)
        
    st.divider()
    st.caption("Data sourced from the Global Legal Entity Identifier Foundation (GLEIF) API.")


if __name__ == "__main__":
    main()