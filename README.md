# Climate Data Retrieval

Tools for downloading, processing, and visualizing climate reanalysis data from Copernicus Climate Data Store (CDS).

## Installation

```bash
pip install -r requirements.txt
```

## Features

- **Data Download**: Automated ERA5 and ERA5-Land data retrieval from CDS
- **Metadata Exploration**: Dataset discovery and variable information
- **Visualization**: Geographic maps with coastlines, borders, and projections
- **Multiple Datasets**: Support for regular ERA5, ERA5-Land, and daily statistics

## Quick Start

### 1. Download Data
```python
from piscis import download_data

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

download_data(dataset, params, 'data/raw/era5_temperature.nc')
```

### 2. Explore Metadata
```python
from piscis import show_metadata, search_datasets

# Show file metadata
show_metadata('./data/raw/era5_temperature.nc')

# Search available CDS datasets
search_datasets("temperature")
```

### 3. Visualize Data
```python
from piscis import plot_variable, plot_time_series

file_path = './data/raw/era5_temperature.nc'

# Spatial plot with geographic context
plot_variable(file_path, 't2m', time_index=0)

# Time series at location
plot_time_series(file_path, 't2m', lat=40.0, lon=-74.0)
```

## Available Functions

**Download**: `download_data`

**Metadata**: `show_metadata`, `search_datasets`, `get_detailed_dataset_info`

**Visualization**: `plot_variable`, `plot_multiple_variables`, `plot_time_series`, `plot_climatology`, `plot_statistics_summary`

**Utilities**: `nc_loader`, `get_variable_names`

## Examples

See notebook examples in:
- `usage_downloader.ipynb` - Data downloading examples
- `usage_metadata.ipynb` - Dataset exploration
- `usage_visualizer.ipynb` - Visualization examples

## Requirements

Requires CDS API credentials configured. See: https://cds.climate.copernicus.eu/api-how-to

