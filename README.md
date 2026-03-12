# Climate Data Retrieval — `piscis`

Python package for downloading, processing and visualizing climate reanalysis data.
Part of the Suyana data pipeline: **Lead Parameters → Data Request Service → ETL → DB**.

---

## Setup

### 1. Clone and install dependencies
```bash
git clone <repo-url>
cd climate_data_retrieval
pip install -r requirements.txt
```

### 2. Configure credentials
```bash
cp .env.example .env
# Edit .env and fill in your CDS API key and (optionally) AWS credentials
```

**CDS API** (required for ERA5 downloads): get your key at https://cds.climate.copernicus.eu/api-how-to

**AWS** (optional, only if uploading outputs to S3): set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

---

## Data Request Service

The main pipeline. Takes a peril + AOI and downloads the correct climate data automatically.

```python
from piscis import DataRequestService, DataRequest

request = DataRequest(
    lead_id="lead_001",
    peril="precipitation",       # heatwave | cold_spell | drought | precipitation
    aoi={"minx": -77.0, "maxx": -74.0, "miny": -13.0, "maxy": -10.0},
    output_dir="outputs/lead_001",
)

service = DataRequestService()
result = service.run(request)

print(result.nc_files)       # list of downloaded .nc files
print(result.summary_path)   # path to summary.json
```

### Supported perils and sources

| Peril | Source(s) | Variables |
|-------|-----------|-----------|
| `precipitation` | CHIRPS v3 or ERA5-Land | daily precipitation |
| `drought` | ERA5-Land | soil water, precipitation, temperature |
| `heatwave` | ERA5 | 2m temperature, daily max |
| `cold_spell` | ERA5 | 2m temperature, daily min |

When a peril has multiple sources, use `source_filter` to pick one:
```python
# For precipitation: choose between "chirps" (default) or "era5_land"
request = DataRequest(
    peril="precipitation",
    source_filter="chirps",   # or "era5_land"
    ...
)

# See all available sources for a peril
from piscis import list_sources
list_sources("precipitation")   # → ["chirps", "era5_land"]
```

### Historical period
The period is auto-computed as the most recent 30-year window at multiples of 5 (e.g. 1995–2025).
You can also set it manually:
```python
DataRequest(..., period=(1990, 2020))
```

### Upload to S3 (optional)
```python
DataRequest(
    ...,
    s3_bucket="suyana-climate-data",
    s3_prefix="outputs/lead_001",
    s3_region="us-east-1",
)
```

### Run from a YAML file
```python
service.run_from_yaml("configs/request_template.yml")
```
See `configs/request_template.yml` for a full template.

---

## Lower-level tools

```python
from piscis import download_data, show_metadata, plot_variable
```

| Function | What it does |
|----------|-------------|
| `download_data` | Download ERA5 data directly via CDS API |
| `show_metadata` | Inspect a .nc file |
| `search_datasets` | Search available CDS datasets |
| `plot_variable` | Spatial map from a .nc file |
| `plot_time_series` | Time series at a location |

---

## Usage notebooks

| Notebook | Description |
|----------|-------------|
| `usage_data_request_service.ipynb` | Full pipeline walkthrough (start here) |
| `usage_downloader.ipynb` | ERA5 low-level download examples |
| `usage_metadata.ipynb` | Dataset exploration |
| `usage_visualizer.ipynb` | Visualization examples |

---

## Project structure

```
piscis/
├── service.py          # DataRequestService — main orchestrator
├── aoi.py              # AOI parsing (bounding box or shapefile)
├── period.py           # Historical period computation
├── peril_config.py     # Peril → source/variable mapping
├── chirps_downloader.py
├── era5_downloader.py
├── s3_storage.py
├── summary.py
├── downloader.py       # Low-level ERA5 wrapper
├── metadata.py
├── visualizer.py
└── utils.py

configs/
├── perils/             # YAML docs per peril
└── request_template.yml
```

---

## Adding a new data source

1. Create `piscis/<source>_downloader.py`
2. Add `source_type="<source>"` to the relevant peril(s) in `piscis/peril_config.py`
3. Add an `elif` block in `piscis/service.py` → `_dispatch_download()`

See the CHIRTS TODO in `service.py` for a concrete example.
