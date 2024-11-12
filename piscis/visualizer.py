import xarray as xr
import matplotlib.pyplot as plt

def plot_variable(file_path, variable_name, time_index=0):
    """
    plots a snapshot (2d lat-lon) of a specified variable from a .nc dataset, adapting to different dimension names.

    Parameters:
        file_path (str): Path to the data file (.nc).
        variable_name (str): Name of the variable to plot.
        time_index (int): Time index to use for the plot (default is 0).

    Returns:
        None: Displays a plot.
    """
    try:
        # Load the dataset
        ds = xr.open_dataset(file_path)
        
        # Check if variable exists
        if variable_name not in ds.variables:
            raise ValueError(f"Variable '{variable_name}' not found in dataset.")
        
        # Get the variable data
        data = ds[variable_name]
        
        # Identify time, latitude, and longitude dimensions
        time_dim = next((dim for dim in data.dims if "time" in dim), None)
        lat_dim = next((dim for dim in data.dims if "lat" in dim or "latitude" in dim), None)
        lon_dim = next((dim for dim in data.dims if "lon" in dim or "longitude" in dim), None)
        
        # If time_dim is present, select the specified time slice
        if time_dim:
            data = data.isel({time_dim: time_index})
        
        # Create a plot
        plt.figure(figsize=(10, 6))
        data.plot(cmap="viridis")  # Change colormap if desired
        plt.title(f"{variable_name} at time index {time_index}")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.show()
        
        # Close the dataset
        ds.close()
    
    except Exception as e:
        print(f"Error plotting variable {variable_name}: {e}")
