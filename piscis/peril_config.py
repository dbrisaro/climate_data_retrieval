"""
Peril configuration: maps each peril to the data sources and variables needed.

Supported perils:
    - drought
    - heatwave
    - cold_spell
    - precipitation

Each peril can have one or more sources. Each source specifies:
    - source_type : 'era5' | 'era5_land' | 'chirps' | 'chirts'
    - dataset     : CDS dataset name (for ERA5 sources)
    - variables   : list of variable names
    - hours       : list of UTC hours to download
    - levels      : pressure levels (None for single-level datasets)
"""

from dataclasses import dataclass, field
from typing import List, Optional


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SourceConfig:
    """Configuration for a single data source within a peril."""
    source_type: str                       # 'era5', 'era5_land', 'chirps', 'chirts'
    variables: List[str]
    dataset: Optional[str] = None          # CDS dataset name (ERA5 sources only)
    hours: List[str] = field(default_factory=lambda: ["00", "06", "12", "18"])
    levels: Optional[List[str]] = None     # Pressure levels (None for surface vars)
    description: str = ""


@dataclass
class PerilConfig:
    """Complete configuration for a single peril."""
    peril: str
    description: str
    sources: List[SourceConfig]


# ---------------------------------------------------------------------------
# Peril configurations
# ---------------------------------------------------------------------------

PERIL_CONFIGS: dict = {

    "drought": PerilConfig(
        peril="drought",
        description="Soil moisture deficit and precipitation deficiency",
        sources=[
            SourceConfig(
                source_type="era5_land",
                dataset="reanalysis-era5-land",
                variables=[
                    "volumetric_soil_water_layer_1",
                    "volumetric_soil_water_layer_2",
                    "total_precipitation",
                    "2m_temperature",
                ],
                hours=["00"],
                description="Soil moisture and temperature from ERA5-Land",
            ),
        ],
    ),

    "heatwave": PerilConfig(
        peril="heatwave",
        description="Extreme heat events and positive temperature anomalies",
        sources=[
            SourceConfig(
                source_type="era5",
                dataset="reanalysis-era5-single-levels",
                variables=[
                    "2m_temperature",
                    "maximum_2m_temperature_since_previous_post_processing",
                ],
                hours=["06", "18"],   # captures daily max/min cycle
                description="Temperature extremes from ERA5",
            ),
            # TODO: add CHIRTS as a high-resolution alternative for heatwave
            #   source_type = "chirts"
            #   variables   = ["Tmax"]   (~5 km daily max temperature)
            #   See service.py _dispatch_download() for implementation steps.
        ],
    ),

    "cold_spell": PerilConfig(
        peril="cold_spell",
        description="Extreme cold events, frost risk, and negative temperature anomalies",
        sources=[
            SourceConfig(
                source_type="era5",
                dataset="reanalysis-era5-single-levels",
                variables=[
                    "2m_temperature",
                    "minimum_2m_temperature_since_previous_post_processing",
                ],
                hours=["06", "18"],
                description="Temperature extremes from ERA5",
            ),
            # TODO: add CHIRTS as a high-resolution alternative for cold_spell
            #   source_type = "chirts"
            #   variables   = ["Tmin"]   (~5 km daily min temperature)
            #   See service.py _dispatch_download() for implementation steps.
        ],
    ),

    "precipitation": PerilConfig(
        peril="precipitation",
        description="Rainfall events, flood risk, and precipitation extremes",
        sources=[
            SourceConfig(
                source_type="chirps",
                variables=["precip"],
                description=(
                    "Daily precipitation from CHIRPS v3.0 (~5 km). "
                    "Best choice for high-resolution rainfall analysis."
                ),
            ),
            SourceConfig(
                source_type="era5_land",
                dataset="reanalysis-era5-land",
                variables=["total_precipitation"],
                hours=["00"],
                description=(
                    "Total precipitation from ERA5-Land (0.1°). "
                    "Use when consistency with other ERA5-Land variables "
                    "is needed (e.g. combined drought + precipitation analysis)."
                ),
            ),
        ],
    ),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_peril_config(peril: str) -> PerilConfig:
    """
    Get the configuration for a given peril.

    Parameters:
        peril: One of 'drought', 'heatwave', 'cold_spell', 'precipitation'.
               Case-insensitive; spaces and hyphens are normalised to underscores.

    Returns:
        PerilConfig object

    Raises:
        ValueError if the peril is not recognised.
    """
    key = peril.lower().replace(" ", "_").replace("-", "_")
    if key not in PERIL_CONFIGS:
        valid = list(PERIL_CONFIGS.keys())
        raise ValueError(
            f"Unknown peril '{peril}'. Valid options: {valid}"
        )
    return PERIL_CONFIGS[key]


def list_sources(peril: str) -> List[str]:
    """
    Return the available source types for a given peril.

    Example:
        list_sources("precipitation")  →  ["chirps", "era5_land"]
        list_sources("drought")        →  ["era5_land"]
    """
    cfg = get_peril_config(peril)
    return [s.source_type for s in cfg.sources]


def list_perils() -> List[str]:
    """Return the list of supported peril names."""
    return list(PERIL_CONFIGS.keys())
