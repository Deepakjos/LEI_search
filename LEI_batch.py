import streamlit as st
import requests
import pandas as pd
import time
import re
from typing import List, Dict, Any, Union

# --- Constants ---
BASE_URL = "https://api.gleif.org/api/v1/lei-records"
MAX_BATCH_SIZE = 200
REQUESTS_PER_MINUTE_LIMIT = 60
DEFAULT_TIMEOUT = 20  # seconds

# A list of common legal suffixes and their variations to be removed for search
LEGAL_SUFFIXES_TO_REMOVE = sorted([
    'PVT. LTD.', 'PRIVATE LIMITED', 'P. LTD.', 'PVT.', 'LTD.', 'INC.', 'LLC', 'LIMITED',
    'PRIVATE', 'CORPORATION', 'CORP.', 'CO.', 'COMPANY', 'GMBH', 'S.A.',
    'B.V.', 'A.S.', 'SA', 'BV', 'AS'
], key=len, reverse=True)


# --- Utility Functions ---
def clean_legal_name(name: str) -> str:
    """
    Cleans a legal entity name by removing common legal suffixes and normalizing spaces.
    Handles variations like 'PVT. LTD.', 'PRIVATE LIMITED', etc.
    """
    name = name.upper()
    name = re.sub(r'[.,]', '', name)  # remove punctuation

    for suffix in LEGAL_SUFFIXES_TO_REMOVE:
        # Remove suffixes appearing anywhere (end or middle)
        name = re.sub(rf'\b{re.escape(suffix.replace(".", ""))}\b', '', name)

    # Normalize whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def safe_get(data: Dict[str, Any], path: str, default: Any = "N/A") -> Any:
    """Safely retrieve a nested value from a dictionary using a dot-separated path."""
    keys = path.split('.')
    for key in keys:
        if isinstance(data, dict):
            try:
                if key.isdigit():
                    data = data[int(key)]
                else:
                    data = data.get(key)
            except (IndexError, KeyError, TypeError):
                return default
        else:
            return default
    return data if data is not None else default


def format_address(addr: Dict[str, Any]) -> str:
    """Formats an address dictionary into a single comma-separated string."""
    if not isinstance(addr, dict):
        return "N/A"

    parts = [
        safe_get(addr, 'addressLines.0', ''),
        safe_get(addr, 'city', ''),
        safe_get(addr, 'region', ''),
        safe_get(addr, 'postalCode', ''),
        safe_get(addr, 'country', '')
    ]
    return ", ".join(filter(None, parts)) or "N/A"


def parse_api_record(record: Dict[str, Any], search_query: str = None) -> Dict[str, Any]:
    """Normalizes a single LEI record from the API into a flat dictionary for display."""
    base_data = {
        "LEI": safe_get(record, 'id'),
        "Legal Name": safe_get(record, 'attributes.entity.legalName.name'),
        "Other Name": safe_get(record, 'attributes.entity.otherEntityNames.0.otherEntityName'),
        "Entity Status": safe_get(record, 'attributes.entity.status'),
        "Legal Address": format_address(safe_get(record, 'attributes.entity.legalAddress', {})),
        "Registration Status": safe_get(record, 'attributes.registration.status'),
        "Managing LOU": safe_get(record, 'attributes.registration.managingLou'),
        "LEI Next Renewal Date": safe_get(record, 'attributes.registration.nextRenewalDate'),
        "Entity Creation Date": safe_get(record, 'attributes.entity.entityCreationDate'),
        "Entity Expiration Date": safe_get(record, 'attributes.entity.entityExpirationDate'),
        "Registered At (Register)": safe_get(record, 'attributes.entity.registeredAt.id'),
        "Registered As": safe_get(record, 'attributes.entity.registeredAs'),
        "Registration Authority Entity ID": safe_get(record, 'attributes.entity.registrationAuthority.registrationAuthorityEntityID'),
        "Validation Authority Entity ID (primary)": safe_get(record, 'attributes.registration.validationAuthority.validationAuthorityEntityID'),
        "Other Validation Authority ID 1": safe_get(record, 'attributes.registration.otherValidationAuthorities.0.validationAuthorityEntityID'),
        "Other Validation Authority ID 2": safe_get(record, 'attributes.registration.otherValidationAuthorities.1.validationAuthorityEntityID'),
        "Other Validation Authority ID 3": safe_get(record, 'attributes.registration.otherValidationAuthorities.2.validationAuthorityEntityID'),
        "Other Validation Authority ID 4": safe_get(record, 'attributes.registration.otherValidationAuthorities.3.validationAuthorityEntityID'),
        "Other Validation Authority ID 5": safe_get(record, 'attributes.registration.otherValidationAuthorities.4.validationAuthorityEntityID'),
    }

    if search_query:
        return {"Search Query": search_query, **base_data}
    return base_data


# --- GLEIF API Client ---
class GleifClient:
    """A client to interact with the GLEIF API, with rate-limiting and error handling."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/vnd.api+json"})

    def _make_request(self, params: Dict[str, Any], url: str = BASE_URL) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        try:
            response = self.session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            return response.json().get("data", [])
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                st.error("API Error 400 - Invalid filter or parameter. Check your input.")
            elif e.response.status_code == 404:
                st.warning("No records found for the given query.")
            else:
                st.error(f"API Error: {e.response.status_code} - {e.response.text}")
        except requests.exceptions.RequestException as e:
            st.error(f"Request failed: {e}")
        return []

    def _make_search_request(self, queries: List[str], filter_key: str) -> List[Dict[str, Any]]:
        """Unified method to perform searches with rate-limiting and deduplication."""
        all_results = []
        seen_leis = set()
        status_text = st.empty()

        for i, query in enumerate(queries, 1):
            status_text.info(f"Processing query {i}/{len(queries)}: '{query}'...")
            params = {filter_key: query.strip(), "page[size]": 10}
            results = self._make_request(params)

            for rec in results:
                parsed_rec = parse_api_record(rec, search_query=query)
                if parsed_rec["LEI"] not in seen_leis:
                    all_results.append(parsed_rec)
                    seen_leis.add(parsed_rec["LEI"])

            if len(queries) > REQUESTS_PER_MINUTE_LIMIT and i < len(queries):
                time.sleep(1)

        status_text.empty()
        return all_results

    def fetch_by_ids(self, lei_ids: List[str]) -> List[Dict[str, Any]]:
        return self._make_search_request(lei_ids, "filter[lei]")

    def search_by_names(self, names: List[str]) -> List[Dict[str, Any]]:
        """
        Improved search by names with multi-stage fallback:
        1️⃣ Exact name
        2️⃣ Cleaned name (remove suffixes)
        3️⃣ Substring fallback (first few tokens if still unmatched)
        """
        all_results = []
        seen_leis = set()
        status_text = st.empty()

        for i, original_name in enumerate(names, 1):
            status_text.info(f"🔍 Searching {i}/{len(names)}: '{original_name}'")
            found = False

            def fetch_and_store(query, tag):
                nonlocal found
                params = {"filter[fulltext]": query.strip(), "page[size]": 10}
                results = self._make_request(params)
                for rec in results:
                    parsed_rec = parse_api_record(rec, search_query=f"{original_name} ({tag})")
                    if parsed_rec["LEI"] not in seen_leis:
                        all_results.append(parsed_rec)
                        seen_leis.add(parsed_rec["LEI"])
                        found = True

            # Step 1: Exact name
            fetch_and_store(original_name, "exact")

            # Step 2: Cleaned name
            if not found:
                cleaned = clean_legal_name(original_name)
                if cleaned != original_name.upper():
                    status_text.info(f"Fallback → Cleaned: '{cleaned}'")
                    fetch_and_store(cleaned, "cleaned")

            # Step 3: Substring fallback
            if not found:
                cleaned = clean_legal_name(original_name)
                tokens = cleaned.split()
                if len(tokens) > 2:
                    substring_query = " ".join(tokens[:min(3, len(tokens))])
                    status_text.info(f"Fallback → Substring: '{substring_query}'")
                    fetch_and_store(substring_query, "substring")

            if len(names) > REQUESTS_PER_MINUTE_LIMIT and i < len(names):
                time.sleep(1)

        status_text.empty()
        return all_results

    def search_by_validation_ids(self, validation_ids: List[str]) -> List[Dict[str, Any]]:
        return self._make_search_request(validation_ids, "filter[fulltext]")


# --- UI Rendering Functions ---
def render_results(results: List[Dict[str, Any]], file_name: str):
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
    st.header("Batch Search by LEI ID")
    st.markdown("Upload a CSV file where the first column contains the 20-character LEI IDs.")

    uploaded_file = st.file_uploader("Upload LEI CSV", type=["csv"], key="lei_uploader")
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file, dtype=str)
            lei_list = [lei.strip().upper() for lei in df.iloc[:, 0].dropna() if len(lei.strip()) == 20]

            st.info(f"Found {len(lei_list)} valid LEI IDs in the uploaded file.")
            if st.button("Start LEI Batch Search", use_container_width=True):
                with st.spinner("Fetching data from GLEIF API..."):
                    results = client.fetch_by_ids(lei_list)
                    render_results(results, "lei_batch_results.csv")
        except Exception as e:
            st.error(f"Failed to process CSV file. Error: {e}")


def render_name_tab(client: GleifClient):
    st.header("Batch Search by Legal Name")
    st.markdown("Upload a CSV file where the first column contains the legal names.")

    uploaded_file = st.file_uploader("Upload Legal Name CSV", type=["csv"], key="name_uploader")
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file, dtype=str)
            name_list = [name.strip() for name in df.iloc[:, 0].dropna() if len(name.strip()) >= 3]

            st.info(f"Found {len(name_list)} valid names (min. 3 chars) in the file.")
            if st.button("Start Name Batch Search", use_container_width=True):
                with st.spinner("Searching GLEIF database by name... This may take a while."):
                    results = client.search_by_names(name_list)
                    render_results(results, "name_batch_results.csv")
        except Exception as e:
            st.error(f"Failed to process CSV file. Error: {e}")


def render_id_tab(client: GleifClient):
    st.header("Batch Search by Registration/Validation Authority ID")
    st.markdown("Upload a CSV file where the first column contains the IDs.")

    uploaded_file = st.file_uploader("Upload ID CSV", type=["csv"], key="id_uploader")
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file, dtype=str)
            id_list = [id_val.strip() for id_val in df.iloc[:, 0].dropna() if id_val.strip()]

            st.info(f"Found {len(id_list)} IDs to search.")
            if st.button("Start ID Batch Search", use_container_width=True):
                with st.spinner("Searching GLEIF database by ID..."):
                    results = client.search_by_validation_ids(id_list)
                    render_results(results, "id_batch_results.csv")
        except Exception as e:
            st.error(f"Failed to process CSV file. Error: {e}")


# --- Main App ---
def main():
    st.set_page_config(page_title="GLEIF LEI Search Tool", layout="wide", initial_sidebar_state="collapsed")
    st.title("🏛️ Global LEI Index Search (Batch Enabled)")
    st.markdown(
        "Use the tabs below to search for Legal Entity Identifiers (LEIs) via the official GLEIF API. "
        "Batch searches include suffix-insensitive name matching and substring fallback."
    )

    client = GleifClient()
    tab1, tab2, tab3 = st.tabs(["Search by LEI ID", "Search by Legal Name", "Search by Registration ID"])

    with tab1:
        render_lei_tab(client)
    with tab2:
        render_name_tab(client)
    with tab3:
        render_id_tab(client)

    st.divider()
    st.caption("Data sourced from the Global Legal Entity Identifier Foundation (GLEIF) API.")


if __name__ == "__main__":
    main()
