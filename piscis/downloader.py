import cdsapi
import os
from .utils import check_file_exists

def download_data(dataset_name, params, save_path):
    """
    Downloads data from the CDS API if it's not already available locally.

    Parameters:
        dataset_name (str): name of the dataset in the CDS API.
        params (dict): parameters for the data request (e.g., variables, dates).
        save_path (str): local path to save the downloaded data file.
    """
    if check_file_exists(save_path):
        print(f"Data already exists at {save_path}. Skipping download.")
        return save_path

    # connect to the CDS API
    client = cdsapi.Client()
    print(f"Downloading {dataset_name} with parameters {params}")

    # request and download data
    client.retrieve(dataset_name, params, save_path)
    print(f"Data downloaded and saved to {save_path}")
    
    return save_path
