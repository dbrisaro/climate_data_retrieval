import requests
import json
import os
from datetime import datetime, timedelta

CACHE_FILE = "dataset_cache.json"  # Cache file stored in the project root
CACHE_DURATION = timedelta(days=30)  # Set cache validity duration

def is_cache_valid():
    """
    Check if the cache file exists and is recent enough.
    
    Returns:
        bool: True if cache is valid, False if it is not
    """

    if not os.path.exists(CACHE_FILE):
        return False
    
    # check last modified time of cache file
    last_modified = datetime.fromtimestamp(os.path.getmtime(CACHE_FILE))
    return datetime.now() - last_modified < CACHE_DURATION

def fetch_datasets():
    """
    Fetch dataset metadata from the CDS API and cache it.
    
    Returns:
        dict: Dictionary of dataset metadata.
    """
    # use cache file if it's valid
    if is_cache_valid():
        with open(CACHE_FILE, "r") as file:
            print("Loading datasets from cache.")
            return json.load(file)
    
    # fetch new metadata from the API if cache is invalid
    url = "https://cds.climate.copernicus.eu/api/catalogue/v1/collections"
    response = requests.get(url)
    
    if response.status_code != 200:
        raise ConnectionError(f"Failed to fetch datasets. Status code: {response.status_code}")
    
    data = response.json()
    datasets = {}

    # collections into a dictionary
    for collection in data.get("collections", []):
        dataset_id = collection.get("id")
        title = collection.get("title")
        description = collection.get("description")
        datasets[dataset_id] = {
            "title": title,
            "description": description
        }

    with open(CACHE_FILE, "w") as file:
        json.dump(datasets, file)
    
    print("Fetched datasets from the API and updated cache.")
    return datasets

def check_for_new_datasets():
    """
    Checks for any new datasets that have been added since the last cache update.
    
    Returns:
        list: List of new dataset IDs, if any.
    """
    # Fetch cached data if valid, otherwise return an empty dict
    cached_data = fetch_datasets() if is_cache_valid() else {}
    
    # Fetch the latest data
    live_data = fetch_datasets()
    
    # Identify new datasets by comparing keys
    new_datasets = [ds_id for ds_id in live_data if ds_id not in cached_data]
    
    if new_datasets:
        print(f"New datasets found: {new_datasets}")
    else:
        print("No new datasets found.")
    
    return new_datasets

def search_datasets(keyword, datasets_dict):
    """
    Search for datasets containing a keyword in their ID, title, or description.
    
    Args:
        keyword (str): Keyword to search for
        datasets_dict (dict): Dictionary of datasets
        
    Returns:
        dict: Datasets matching the search criteria
    """
    matching_datasets = {}
    keyword_lower = keyword.lower()
    
    for dataset_id, metadata in datasets_dict.items():
        id_match = keyword_lower in dataset_id.lower()
        title_match = keyword_lower in metadata.get('title', '').lower()
        desc_match = keyword_lower in metadata.get('description', '').lower()
        
        if id_match or title_match or desc_match:
            matching_datasets[dataset_id] = metadata
    
    return matching_datasets

def get_detailed_dataset_info(dataset_id):
    """
    Get detailed information for a specific dataset from the API.
    
    Args:
        dataset_id (str): Dataset ID
        
    Returns:
        dict: Detailed dataset information
    """
    try:
        url = f"https://cds.climate.copernicus.eu/api/catalogue/v1/collections/{dataset_id}"
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception:
        return None

def show_dataset_metadata(dataset_id, datasets_dict):
    """
    Display dataset metadata in an organized format.
    
    Args:
        dataset_id (str): Dataset ID
        datasets_dict (dict): Basic datasets dictionary
    """
    if dataset_id not in datasets_dict:
        print(f"Dataset '{dataset_id}' not found in cache.")
        return
    
    print(f"METADATA FOR: {dataset_id}")
    print("="*60)
    
    basic_info = datasets_dict[dataset_id]
    print(f"Title: {basic_info.get('title', 'N/A')}")
    print(f"Description: {basic_info.get('description', 'N/A')}")
    
    detailed_info = get_detailed_dataset_info(dataset_id)
    
    if detailed_info:
        print(f"\nTECHNICAL INFO:")
        
        if 'variables' in detailed_info:
            print(f"   Variables: {', '.join(detailed_info['variables'].keys())}")
        
        if 'extent' in detailed_info and 'temporal' in detailed_info['extent']:
            temporal = detailed_info['extent']['temporal']
            print(f"   Temporal extent: {temporal}")
        
        if 'extent' in detailed_info and 'spatial' in detailed_info['extent']:
            spatial = detailed_info['extent']['spatial']
            print(f"   Spatial extent: {spatial}")
        
        if 'keywords' in detailed_info:
            print(f"   Keywords: {', '.join(detailed_info['keywords'])}")
    
    print("="*60)
