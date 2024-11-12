import xarray as xr

def calculate_climatology(file_path, variable_name, time_period="monthly"):
    """
    Calculates climatology (average over time) for a specified variable.

    Parameters:
        file_path (str): Path to the raw data file (NetCDF format).
        variable_name (str): The variable to calculate climatology for (e.g., temperature).
        time_period (str): The time period for climatology; options are "monthly", "seasonal", or "annual".

    Returns:
        climatology (xr.DataArray) Climatology data array.
    """
    # Load the dataset
    ds = xr.open_dataset(file_path)
    data = ds[variable_name]

    # Calculate climatology based on specified time period
    if time_period == "monthly":
        climatology = data.groupby("time.month").mean(dim="time")
    elif time_period == "seasonal":
        # Use xarray's groupby with season
        climatology = data.groupby("time.season").mean(dim="time")
    elif time_period == "annual":
        climatology = data.groupby("time.year").mean(dim="time").mean(dim="year")
    else:
        raise ValueError("Invalid time period. Choose from 'monthly', 'seasonal', or 'annual'.")

    ds.close()
    return climatology
