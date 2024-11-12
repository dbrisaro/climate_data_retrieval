import os
import xarray as xr

## es esta funcion medio al pedo?
def check_file_exists(file_path):
    """
    Checks if a file exists at the specified path. 

    Parameters:
        file_path (str): Path to the file.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    return os.path.isfile(file_path)

## es esta funcion medio al pedo?
def nc_loader(file_path):
    """
    Loads a .nc file if it exists and returns it as an xarray.Dataset.

    Parameters:
        file_path (str): Path to the .nc file.

    Returns:
        xarray.Dataset: The loaded dataset if successful.
        None: If the file does not exist or there’s an error loading it.
    """
    # Check if file exists
    if not os.path.isfile(file_path):
        print(f"File not found at {file_path}")
        return None

    # Try loading the .nc file
    try:
        dataset = xr.open_dataset(file_path)
        print(f"Loaded NetCDF file at {file_path}")
        return dataset
    except Exception as e:
        print(f"Error loading NetCDF file {file_path}: {e}")
        return None


## nos muestra la metadata del xarray. capaz estaria bueno tener la metadata en algun formato tipo df? alguna tabla? algo
def show_metadata(file_path):
    """
    Displays metadata for a .nc dataset.

    Parameters:
        file_path (str): Path to the data file (.nc).

    Returns:
        None: Prints metadata to the console.
    """
    try:
        ds = xr.open_dataset(file_path)
        
        # shows global attributes
        print("===== Global Attributes =====")
        print(ds.attrs)
        
        # variables and their metadata
        print("\n===== Variables =====")
        for var_name, var_data in ds.variables.items():
            print(f"\nVariable: {var_name}")
            print(f"  Dimensions: {var_data.dims}")
            print(f"  Shape: {var_data.shape}")
            print(f"  Attributes:")
            for attr, value in var_data.attrs.items():
                print(f"    {attr}: {value}")
                
        ds.close()
    
    except Exception as e:
        print(f"Error opening file {file_path}: {e}")
