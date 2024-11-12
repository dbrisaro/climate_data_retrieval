# climate_data_retrieval

## installation
```
pip install -r requirements.txt
```

## usage

### downloading data
To retrieve ERA5 data (eg 2m temperature) and save it to a specified path, use the following parameters and code example:
```python
# Define dataset and parameters
dataset = "reanalysis-era5-single-levels"
params = {
    "product_type": "reanalysis",
    "format": "netcdf",
    "variable": ["2m_temperature"],
    "year": "2020",
    "month": "01",
    "day": "01",
    "time": "12:00",
}

# Specify output path
output_path = 'data/raw/era5_2m_temperature.nc'

# Run the download
from piscis import download_data
download_data(dataset, params, output_path)

```

### showing metadata
After downloading the data, you can load and display its metadata to verify its structure and attributes.

```python
# Define dataset and parameters
# Load and show metadata for ERA5 data
from piscis import nc_loader, show_metadata

file_path = './data/raw/era5_2m_temperature.nc'

data = nc_loader(file_path)
show_metadata(file_path)
```

### plotting data
You can plot a 2D snapshot of the data (e.g., 2m_temperature) at a specific time index using plot_variable.

```python
# Plot ERA5 data: 2m temperature at time index 0
from piscis import plot_variable

file_path = './data/raw/era5_2m_temperature.nc'
variable_name = 't2m'

plot_variable(file_path, variable_name, time_index=0)
```

