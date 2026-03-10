from .downloader import download_data
from .processing import calculate_climatology
from .utils import check_file_exists, show_metadata, nc_loader
from .visualizer import plot_variable, plot_time_series, plot_multiple_variables, plot_variable_animation, plot_climatology, compare_datasets, plot_statistics_summary, print_variables_summary, get_variable_names, select_variable_interactive
from .metadata import is_cache_valid, fetch_datasets, check_for_new_datasets, search_datasets, get_detailed_dataset_info, show_dataset_metadata

# ── Data Request Service ──────────────────────────────────────────────────
from .service import DataRequestService, DataRequest, DataRequestResult
from .aoi import BoundingBox, parse_aoi, aoi_from_dict, aoi_from_shapefile
from .period import compute_period, get_year_list, describe_period
from .peril_config import get_peril_config, list_perils, list_sources, PERIL_CONFIGS
from .chirps_downloader import CHIRPSDownloader
from .era5_downloader import ERA5Downloader
from .summary import generate_summary, save_summary, print_summary

__all__ = [
    # ── existing ──
    "download_data",
    "calculate_climatology",
    "check_file_exists",
    "show_metadata",
    "plot_variable",
    "plot_time_series",
    "plot_multiple_variables",
    "plot_variable_animation",
    "plot_climatology",
    "compare_datasets",
    "plot_statistics_summary",
    "print_variables_summary",
    "get_variable_names",
    "select_variable_interactive",
    "nc_loader",
    "is_cache_valid",
    "fetch_datasets",
    "check_for_new_datasets",
    "search_datasets",
    "get_detailed_dataset_info",
    "show_dataset_metadata",
    # ── Data Request Service ──
    "DataRequestService",
    "DataRequest",
    "DataRequestResult",
    "BoundingBox",
    "parse_aoi",
    "aoi_from_dict",
    "aoi_from_shapefile",
    "compute_period",
    "get_year_list",
    "describe_period",
    "get_peril_config",
    "list_perils",
    "list_sources",
    "PERIL_CONFIGS",
    "CHIRPSDownloader",
    "ERA5Downloader",
    "generate_summary",
    "save_summary",
    "print_summary",
]
