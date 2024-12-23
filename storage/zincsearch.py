import requests
from typing import Dict, Optional, List
import logging
from requests.sessions import Session

BASE_ENDPOINT = "http://localhost:4080"

DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "accept": "application/json"
}

CHROM_COL_NAME = "#CHROM"
FILTER_COL_NAME = "FILTER"
INFO_COL_NAME = "INFO"
FORMAT_COL_NAME = "FORMAT"

INDEXABLE_COLS = [
    CHROM_COL_NAME,
    FILTER_COL_NAME,
    INFO_COL_NAME,
    FORMAT_COL_NAME,
]

# Add a session object as a module-level variable
_session = Session()

def create_index_mapping_from_headers(index_name: str, headers: List[str]) -> dict:
    """
    Create ZincSearch mapping from VCF headers.
    All fields will be searchable and stored.
    """

    properties = {
        "filename": {
            "type": "keyword",
            "index": True,
            "store": False
        }
    }

    for header in headers:
        # Special handling for known columns
        if header in INDEXABLE_COLS:
            if header == CHROM_COL_NAME:
                # Chromosome should be keyword for exact matching
                properties[header] = {
                    "type": "keyword",
                    "index": True,
                    "store": False,
                    "sortable": True,
                    "highlightable": True
                }
            elif header == FILTER_COL_NAME:
                # Filter values should be keywords for aggregations
                properties[header] = {
                    "type": "keyword",
                    "index": True,
                    "store": False,
                    "sortable": True,
                    "highlightable": True
                }
            else:
                # INFO and FORMAT fields as text for flexible searching
                properties[header] = {
                    "type": "text",
                    "index": True,
                    "store": False,
                    "sortable": True,
                    "highlightable": True
                }
        else:
            # Default mapping for other columns
            properties[header] = {
                "type": "text",
                "index": False,
                "store": False
            }

    return {
        "name": index_name,
        "storage_type": "disk",
        "shard_num": 50,
        "mappings": {
            "properties": properties
        },
        "settings": {
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "standard"
                    }
                }
            }
        }
    }

def create_or_update_mapping(
    mapping_data: Dict,
    username: str = "admin",
    password: str = "admin",
    base_url: Optional[str] = None
) -> Dict:
    """
    Create or update mapping for a ZincSearch index.

    Args:
        mapping_data: Dictionary containing the mapping configuration
        username: ZincSearch username (defaults to 'admin')
        password: ZincSearch password (defaults to 'admin')
        base_url: Optional custom base URL (defaults to BASE_ENDPOINT)

    Returns:
        Dict: Response from the ZincSearch API

    Raises:
        requests.exceptions.RequestException: If the API request fails
    """
    url = f"{base_url or BASE_ENDPOINT}/api/index"

    # Use session instead of requests directly
    response = _session.put(
        url,
        json=mapping_data,
        auth=(username, password),
        headers=DEFAULT_HEADERS
    )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 400 and "already exists" in response.text.lower():
            logging.warning(f"Index already exists: {response.text}")
        else:
            raise

    return response.json()

def bulk_insert(
    index_name: str,
    records: List[Dict],
    username: str = "admin",
    password: str = "admin",
    base_url: Optional[str] = None
) -> Dict:
    """
    Bulk insert records into a ZincSearch index.

    Args:
        index_name: Name of the index to insert into
        records: List of dictionaries containing the records to insert
        username: ZincSearch username (defaults to 'admin')
        password: ZincSearch password (defaults to 'admin')
        base_url: Optional custom base URL (defaults to BASE_ENDPOINT)

    Returns:
        Dict: Response from the ZincSearch API

    Raises:
        requests.exceptions.RequestException: If the API request fails
        ValueError: If records is empty
    """
    if not records:
        raise ValueError("Records list cannot be empty")

    url = f"{base_url or BASE_ENDPOINT}/api/_bulkv2"

    payload = {
        "index": index_name,
        "records": records
    }

    # Use session here as well
    response = _session.post(
        url,
        json=payload,
        auth=(username, password),
        headers=DEFAULT_HEADERS
    )

    response.raise_for_status()

    return response.json()

def search_records(
    index_name: str,
    filename: Optional[str] = None,
    search_term: Optional[str] = None,
    page: int = 1,
    size: int = 10,
    username: str = "admin",
    password: str = "admin",
    base_url: Optional[str] = None
) -> Dict:
    """
    Search records in a ZincSearch index with pagination, filename filter, and search across indexable columns.

    Args:
        index_name: Name of the index to search in
        filename: Optional filename to filter results
        search_term: Optional term to search across all indexable columns
        page: Page number (1-based indexing, defaults to 1)
        size: Number of records per page (defaults to 10)
        username: ZincSearch username (defaults to 'admin')
        password: ZincSearch password (defaults to 'admin')
        base_url: Optional custom base URL (defaults to BASE_ENDPOINT)

    Returns:
        Dict: Response from the ZincSearch API containing search results and metadata
    """
    if page < 1:
        raise ValueError("Page number must be greater than 0")
    if size < 1:
        raise ValueError("Page size must be greater than 0")

    url = f"{base_url or BASE_ENDPOINT}/es/{index_name}/_search"
    offset = (page - 1) * size

    # Start with a match_all query by default
    query = {
        "from": offset,
        "size": size,
        "query": {
            "match_all": {}
        }
    }

    # If we have search conditions, switch to bool query
    if filename or search_term:
        query["query"] = {
            "bool": {}
        }

        # Add filename filter if provided
        if filename:
            if "must" not in query["query"]["bool"]:
                query["query"]["bool"]["must"] = []
            query["query"]["bool"]["must"].append({
                "term": {
                    "filename": filename
                }
            })

        # Add search across indexable columns if search term is provided
        if search_term:
            if "should" not in query["query"]["bool"]:
                query["query"]["bool"]["should"] = []

            for field in INDEXABLE_COLS:
                # Add prefix query for partial matches at start
                query["query"]["bool"]["should"].append({
                    "prefix": {
                        field: search_term
                    }
                })

                # Add wildcard query for partial matches anywhere
                query["query"]["bool"]["should"].append({
                    "wildcard": {
                        field: f"*{search_term}*"
                    }
                })

                # Add match query for text analysis
                query["query"]["bool"]["should"].append({
                    "match": {
                        field: {
                            "query": search_term,
                            "fuzziness": "AUTO"
                        }
                    }
                })

            query["query"]["bool"]["minimum_should_match"] = 1

    # Original search request
    response = _session.post(
        url,
        json=query,
        auth=(username, password),
        headers=DEFAULT_HEADERS
    )

    response.raise_for_status()
    return response.json()