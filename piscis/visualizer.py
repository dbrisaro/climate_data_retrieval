import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import pandas as pd
from .utils import nc_loader, show_metadata

try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    HAS_CARTOPY = True
except ImportError:
    HAS_CARTOPY = False

def _has_spatial_coords(data):
    """Check if data has latitude and longitude coordinates."""
    lat_dim = next((dim for dim in data.dims if "lat" in dim or "latitude" in dim), None)
    lon_dim = next((dim for dim in data.dims if "lon" in dim or "longitude" in dim), None)
    return lat_dim is not None and lon_dim is not None

def _get_spatial_dims(data):
    """Get latitude and longitude dimension names."""
    lat_dim = next((dim for dim in data.dims if "lat" in dim or "latitude" in dim), None)
    lon_dim = next((dim for dim in data.dims if "lon" in dim or "longitude" in dim), None)
    return lat_dim, lon_dim

def plot_variable(file_path, variable_name, time_index=0):
    """
    plots a snapshot (2d lat-lon) of a specified variable from a .nc dataset with geographic context.

    Parameters:
        file_path (str): Path to the data file (.nc).
        variable_name (str): Name of the variable to plot.
        time_index (int): Time index to use for the plot (default is 0).

    Returns:
        None: Displays a plot.
    """
    try:
        # Load the dataset using nc_loader
        ds = nc_loader(file_path)
        if ds is None:
            return
        
        # Check if variable exists
        if variable_name not in ds.variables:
            raise ValueError(f"Variable '{variable_name}' not found in dataset.")
        
        # Get the variable data
        data = ds[variable_name]
        
        # Identify time, latitude, and longitude dimensions
        time_dim = next((dim for dim in data.dims if "time" in dim), None)
        lat_dim, lon_dim = _get_spatial_dims(data)
        
        # If time_dim is present, select the specified time slice
        if time_dim:
            data = data.isel({time_dim: time_index})
        
        # Create plot with geographic context if possible
        if HAS_CARTOPY and _has_spatial_coords(data):
            fig, ax = plt.subplots(figsize=(12, 8), 
                                 subplot_kw={'projection': ccrs.PlateCarree()})
            
            # Plot the data
            im = data.plot(ax=ax, cmap="viridis", transform=ccrs.PlateCarree(),
                          add_colorbar=True, cbar_kwargs={'shrink': 0.7})
            
            # Add geographic features
            ax.add_feature(cfeature.COASTLINE, linewidth=0.8)
            ax.add_feature(cfeature.BORDERS, linewidth=0.5)
            ax.add_feature(cfeature.OCEAN, color='lightblue', alpha=0.3)
            ax.add_feature(cfeature.LAND, color='lightgray', alpha=0.2)
            
            # Set extent based on data
            lon_coords = data.coords[lon_dim]
            lat_coords = data.coords[lat_dim]
            ax.set_extent([float(lon_coords.min()), float(lon_coords.max()),
                          float(lat_coords.min()), float(lat_coords.max())], 
                         ccrs.PlateCarree())
            
            # Add gridlines
            ax.gridlines(draw_labels=True, alpha=0.5)
            ax.set_title(f"{variable_name} at time index {time_index}")
            
        else:
            # Fallback to basic matplotlib if cartopy not available
            fig, ax = plt.subplots(figsize=(10, 6))
            data.plot(ax=ax, cmap="viridis")
            ax.set_title(f"{variable_name} at time index {time_index}")
            ax.set_xlabel("Longitude")
            ax.set_ylabel("Latitude")
        
        plt.tight_layout()
        plt.show()
        
        # Close the dataset
        ds.close()
    
    except Exception as e:
        print(f"Error plotting variable {variable_name}: {e}")


def plot_time_series(file_path, variable_name, lat=None, lon=None, method='nearest'):
    """
    Plot time series of a variable at a specific location or area average.
    
    Parameters:
        file_path (str): Path to the data file (.nc).
        variable_name (str): Name of the variable to plot.
        lat (float, optional): Latitude for point extraction.
        lon (float, optional): Longitude for point extraction.
        method (str): Selection method ('nearest', 'mean').
    
    Returns:
        None: Displays a plot.
    """
    try:
        ds = nc_loader(file_path)
        if ds is None:
            return
        
        if variable_name not in ds.variables:
            raise ValueError(f"Variable '{variable_name}' not found in dataset.")
        
        data = ds[variable_name]
        
        # Identify dimensions
        time_dim = next((dim for dim in data.dims if "time" in dim), None)
        lat_dim = next((dim for dim in data.dims if "lat" in dim or "latitude" in dim), None)
        lon_dim = next((dim for dim in data.dims if "lon" in dim or "longitude" in dim), None)
        
        if not time_dim:
            raise ValueError("No time dimension found in the data.")
        
        # Extract time series
        if lat is not None and lon is not None:
            if method == 'nearest':
                data = data.sel({lat_dim: lat, lon_dim: lon}, method='nearest')
            else:
                # Area average around the point
                data = data.sel({lat_dim: slice(lat-0.5, lat+0.5), 
                               lon_dim: slice(lon-0.5, lon+0.5)}).mean(dim=[lat_dim, lon_dim])
        else:
            # Global average
            spatial_dims = [dim for dim in data.dims if dim != time_dim]
            data = data.mean(dim=spatial_dims)
        
        # Plot
        plt.figure(figsize=(12, 6))
        data.plot(marker='o', markersize=2)
        title = f"{variable_name} Time Series"
        if lat is not None and lon is not None:
            title += f" at ({lat}, {lon})"
        else:
            title += " (Global Average)"
        plt.title(title)
        plt.xlabel("Time")
        plt.ylabel(f"{variable_name}")
        plt.grid(True, alpha=0.3)
        plt.show()
        
        ds.close()
    
    except Exception as e:
        print(f"Error plotting time series: {e}")

def plot_multiple_variables(file_path, variable_names, time_index=0, figsize=(15, 10)):
    """
    Plot multiple variables in subplots with geographic context.
    
    Parameters:
        file_path (str): Path to the data file (.nc).
        variable_names (list): List of variable names to plot.
        time_index (int): Time index to use for the plot.
        figsize (tuple): Figure size.
    
    Returns:
        None: Displays a plot.
    """
    try:
        ds = nc_loader(file_path)
        if ds is None:
            return
        
        n_vars = len(variable_names)
        ncols = 2 if n_vars > 1 else 1
        nrows = (n_vars + 1) // 2
        
        # Use geographic projection if available and data has spatial coords
        use_geo = HAS_CARTOPY and any(_has_spatial_coords(ds[var]) for var in variable_names if var in ds.variables)
        
        if use_geo:
            fig, axes = plt.subplots(nrows, ncols, figsize=figsize,
                                   subplot_kw={'projection': ccrs.PlateCarree()})
        else:
            fig, axes = plt.subplots(nrows, ncols, figsize=figsize)
        
        if n_vars == 1:
            axes = [axes]
        elif n_vars == 2:
            axes = axes if isinstance(axes, np.ndarray) else [axes]
        else:
            axes = axes.flatten() if isinstance(axes, np.ndarray) else [axes]
        
        for i, var_name in enumerate(variable_names):
            if var_name not in ds.variables:
                print(f"Variable '{var_name}' not found, skipping.")
                continue
            
            data = ds[var_name]
            time_dim = next((dim for dim in data.dims if "time" in dim), None)
            
            if time_dim:
                data = data.isel({time_dim: time_index})
            
            ax = axes[i] if n_vars > 1 else axes[0]
            
            if use_geo and _has_spatial_coords(data):
                im = data.plot(ax=ax, cmap="viridis", transform=ccrs.PlateCarree(), 
                             add_colorbar=True, cbar_kwargs={'shrink': 0.6})
                
                # Add geographic features
                ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
                ax.add_feature(cfeature.BORDERS, linewidth=0.3)
                ax.add_feature(cfeature.OCEAN, color='lightblue', alpha=0.2)
                ax.add_feature(cfeature.LAND, color='lightgray', alpha=0.1)
                
                # Set extent based on data
                lat_dim, lon_dim = _get_spatial_dims(data)
                if lat_dim and lon_dim:
                    lon_coords = data.coords[lon_dim]
                    lat_coords = data.coords[lat_dim]
                    ax.set_extent([float(lon_coords.min()), float(lon_coords.max()),
                                  float(lat_coords.min()), float(lat_coords.max())], 
                                 ccrs.PlateCarree())
                
                # Add gridlines with labels
                ax.gridlines(draw_labels=True, alpha=0.5)
            else:
                im = data.plot(ax=ax, cmap="viridis", add_colorbar=True, 
                             cbar_kwargs={'shrink': 0.6})
                ax.set_xlabel("Longitude")
                ax.set_ylabel("Latitude")
            
            ax.set_title(f"{var_name}")
        
        # Hide unused subplots
        for j in range(n_vars, len(axes)):
            axes[j].set_visible(False)
        
        plt.tight_layout()
        plt.show()
        
        ds.close()
    
    except Exception as e:
        print(f"Error plotting multiple variables: {e}")



def plot_variable_animation(file_path, variable_name, save_path=None, interval=500):
    """
    Create an animation of a variable over time (saves as GIF if save_path provided).
    
    Parameters:
        file_path (str): Path to the data file (.nc).
        variable_name (str): Name of the variable to plot.
        save_path (str, optional): Path to save the animation.
        interval (int): Interval between frames in milliseconds.
    
    Returns:
        None: Displays animation or saves to file.
    """
    try:
        import matplotlib.animation as animation
        
        ds = nc_loader(file_path)
        if ds is None:
            return
        
        if variable_name not in ds.variables:
            raise ValueError(f"Variable '{variable_name}' not found in dataset.")
        
        data = ds[variable_name]
        time_dim = next((dim for dim in data.dims if "time" in dim), None)
        
        if not time_dim:
            raise ValueError("No time dimension found for animation.")
        
        # Set up the figure and axis
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Get data range for consistent colorbar
        vmin, vmax = float(data.min()), float(data.max())
        
        def animate(frame):
            ax.clear()
            data_frame = data.isel({time_dim: frame})
            im = data_frame.plot(ax=ax, vmin=vmin, vmax=vmax, cmap="viridis")
            time_val = data.coords[time_dim].values[frame]
            ax.set_title(f"{variable_name} at {time_val}")
            return [im]
        
        # Create animation
        anim = animation.FuncAnimation(fig, animate, frames=data.sizes[time_dim], 
                                     interval=interval, blit=False, repeat=True)
        
        if save_path:
            anim.save(save_path, writer='pillow')
            print(f"Animation saved to {save_path}")
        else:
            plt.show()
        
        ds.close()
    
    except Exception as e:
        print(f"Error creating animation: {e}")

def plot_climatology(file_path, variable_name, method='monthly'):
    """
    Plot climatology (seasonal cycle) of a variable.
    
    Parameters:
        file_path (str): Path to the data file (.nc).
        variable_name (str): Name of the variable to plot.
        method (str): Climatology method ('monthly', 'seasonal').
    
    Returns:
        None: Displays a plot.
    """
    try:
        ds = nc_loader(file_path)
        if ds is None:
            return
        
        if variable_name not in ds.variables:
            raise ValueError(f"Variable '{variable_name}' not found in dataset.")
        
        data = ds[variable_name]
        time_dim = next((dim for dim in data.dims if "time" in dim), None)
        
        if not time_dim:
            raise ValueError("No time dimension found.")
        
        # Calculate spatial mean
        spatial_dims = [dim for dim in data.dims if dim != time_dim]
        data_mean = data.mean(dim=spatial_dims)
        
        # Group by time period
        if method == 'monthly':
            climatology = data_mean.groupby(f'{time_dim}.month').mean()
            x_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        elif method == 'seasonal':
            climatology = data_mean.groupby(f'{time_dim}.season').mean()
            x_labels = ['DJF', 'MAM', 'JJA', 'SON']
        
        plt.figure(figsize=(10, 6))
        climatology.plot(marker='o', linewidth=2)
        plt.title(f"{variable_name} Climatology ({method.title()})")
        plt.xlabel("Period")
        plt.ylabel(f"{variable_name}")
        plt.grid(True, alpha=0.3)
        
        if method == 'monthly':
            plt.xticks(range(1, 13), x_labels)
        
        plt.show()
        
        ds.close()
    
    except Exception as e:
        print(f"Error plotting climatology: {e}")

def compare_datasets(file_paths, variable_name, labels=None, time_index=0):
    """
    Compare the same variable across multiple datasets with geographic context.
    
    Parameters:
        file_paths (list): List of paths to data files.
        variable_name (str): Name of the variable to compare.
        labels (list, optional): Labels for each dataset.
        time_index (int): Time index to use for comparison.
    
    Returns:
        None: Displays a plot.
    """
    try:
        if labels is None:
            labels = [f"Dataset {i+1}" for i in range(len(file_paths))]
        
        # Check if we can use geographic projection
        use_geo = HAS_CARTOPY
        if use_geo:
            # Test first dataset to see if it has spatial coords
            test_ds = nc_loader(file_paths[0])
            if test_ds and variable_name in test_ds.variables:
                use_geo = _has_spatial_coords(test_ds[variable_name])
                test_ds.close()
            else:
                use_geo = False
        
        if use_geo:
            fig, axes = plt.subplots(1, len(file_paths), figsize=(6*len(file_paths), 4),
                                   subplot_kw={'projection': ccrs.PlateCarree()})
        else:
            fig, axes = plt.subplots(1, len(file_paths), figsize=(5*len(file_paths), 4))
        
        if len(file_paths) == 1:
            axes = [axes]
        
        for i, (file_path, label) in enumerate(zip(file_paths, labels)):
            ds = nc_loader(file_path)
            if ds is None:
                print(f"Could not load {label}, skipping.")
                continue
            
            if variable_name not in ds.variables:
                print(f"Variable '{variable_name}' not found in {label}, skipping.")
                continue
            
            data = ds[variable_name]
            time_dim = next((dim for dim in data.dims if "time" in dim), None)
            
            if time_dim:
                data = data.isel({time_dim: time_index})
            
            ax = axes[i]
            
            if use_geo:
                data.plot(ax=ax, cmap="viridis", transform=ccrs.PlateCarree())
                ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
                ax.add_feature(cfeature.BORDERS, linewidth=0.3)
                ax.gridlines(alpha=0.3)
            else:
                data.plot(ax=ax, cmap="viridis")
            
            ax.set_title(f"{label}")
            ds.close()
        
        plt.suptitle(f"Comparison of {variable_name}")
        plt.tight_layout()
        plt.show()
    
    except Exception as e:
        print(f"Error comparing datasets: {e}")

def plot_statistics_summary(file_path, variable_name):
    """
    Plot statistical summary of a variable (mean, std, min, max over time).
    
    Parameters:
        file_path (str): Path to the data file (.nc).
        variable_name (str): Name of the variable to analyze.
    
    Returns:
        None: Displays plots.
    """
    try:
        ds = nc_loader(file_path)
        if ds is None:
            return
        
        if variable_name not in ds.variables:
            raise ValueError(f"Variable '{variable_name}' not found in dataset.")
        
        data = ds[variable_name]
        time_dim = next((dim for dim in data.dims if "time" in dim), None)
        
        if not time_dim:
            raise ValueError("No time dimension found.")
        
        # Calculate statistics over time
        stats = {
            'Mean': data.mean(dim=time_dim),
            'Std': data.std(dim=time_dim),
            'Min': data.min(dim=time_dim),
            'Max': data.max(dim=time_dim)
        }
        
        # Check if data has spatial coordinates for geographic context
        use_geo = HAS_CARTOPY and _has_spatial_coords(data)
        
        if use_geo:
            fig, axes = plt.subplots(2, 2, figsize=(15, 12),
                                   subplot_kw={'projection': ccrs.PlateCarree()})
        else:
            fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        
        axes = axes.flatten()
        
        for i, (stat_name, stat_data) in enumerate(stats.items()):
            ax = axes[i]
            
            if use_geo:
                im = stat_data.plot(ax=ax, cmap="RdBu_r", transform=ccrs.PlateCarree(),
                                   add_colorbar=True, cbar_kwargs={'shrink': 0.7})
                
                # Add geographic features
                ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
                ax.add_feature(cfeature.BORDERS, linewidth=0.3)
                ax.add_feature(cfeature.OCEAN, color='lightblue', alpha=0.2)
                ax.add_feature(cfeature.LAND, color='lightgray', alpha=0.1)
                
                # Set extent based on data
                lat_dim, lon_dim = _get_spatial_dims(stat_data)
                if lat_dim and lon_dim:
                    lon_coords = stat_data.coords[lon_dim]
                    lat_coords = stat_data.coords[lat_dim]
                    ax.set_extent([float(lon_coords.min()), float(lon_coords.max()),
                                  float(lat_coords.min()), float(lat_coords.max())], 
                                 ccrs.PlateCarree())
                
                # Add gridlines with labels
                ax.gridlines(draw_labels=True, alpha=0.5)
            else:
                stat_data.plot(ax=ax, cmap="RdBu_r")
                ax.set_xlabel("Longitude")
                ax.set_ylabel("Latitude")
            
            ax.set_title(f"{variable_name} - {stat_name}")
        
        plt.suptitle(f"Statistical Summary of {variable_name}")
        plt.tight_layout()
        plt.show()
        
        ds.close()
    
    except Exception as e:
        print(f"Error plotting statistics summary: {e}")

def get_variable_names(file_path):
    """
    Get list of variable names from a dataset using nc_loader.
    
    Parameters:
        file_path (str): Path to the data file (.nc).
    
    Returns:
        list: List of variable names.
    """
    ds = nc_loader(file_path)
    if ds is None:
        return []
    
    var_names = list(ds.data_vars.keys())
    ds.close()
    return var_names

def print_variables_summary(file_path):
    """
    Print a simple summary of available variables, complementing show_metadata.
    
    Parameters:
        file_path (str): Path to the data file (.nc).
    
    Returns:
        list: List of available variable names.
    """
    ds = nc_loader(file_path)
    if ds is None:
        return []
    
    var_names = list(ds.data_vars.keys())
    
    print(f"Available variables in {file_path}:")
    print("-" * 50)
    for i, var_name in enumerate(var_names):
        var_data = ds[var_name]
        long_name = var_data.attrs.get('long_name', 'N/A')
        units = var_data.attrs.get('units', 'N/A')
        print(f"{i}: {var_name} - {long_name} ({units})")
    
    ds.close()
    return var_names

def select_variable_interactive(file_path):
    """
    Interactive variable selection from a dataset.
    
    Parameters:
        file_path (str): Path to the data file (.nc).
    
    Returns:
        str: Selected variable name.
    """
    try:
        available_vars = print_variables_summary(file_path)
        
        if not available_vars:
            print("No variables found in the dataset.")
            return None
        
        if len(available_vars) == 1:
            print(f"Only one variable available: {available_vars[0]}")
            return available_vars[0]
        
        print(f"\nSelect a variable by entering its number (0-{len(available_vars)-1}):")
        
        while True:
            try:
                choice = input("Enter number: ")
                index = int(choice)
                if 0 <= index < len(available_vars):
                    selected = available_vars[index]
                    print(f"Selected: {selected}")
                    return selected
                else:
                    print(f"Please enter a number between 0 and {len(available_vars)-1}")
            except ValueError:
                print("Please enter a valid number")
            except KeyboardInterrupt:
                print("\nSelection cancelled")
                return None
    
    except Exception as e:
        print(f"Error in variable selection: {e}")
        return None
