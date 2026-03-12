from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class SourceConfig:
    source_type: str
    variables: List[str]
    dataset: Optional[str] = None
    hours: List[str] = field(default_factory=lambda: ["00", "06", "12", "18"])
    levels: Optional[List[str]] = None
    description: str = ""


@dataclass
class PerilConfig:
    peril: str
    description: str
    sources: List[SourceConfig]


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
                hours=["06", "18"],
                description="Temperature extremes from ERA5",
            ),
            # TODO: add CHIRTS as high-resolution alternative for heatwave
            #   source_type = "chirts", variables = ["Tmax"]
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
            # TODO: add CHIRTS as high-resolution alternative for cold_spell
            #   source_type = "chirts", variables = ["Tmin"]
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
                description="Daily precipitation from CHIRPS v3.0 (~5 km).",
            ),
            SourceConfig(
                source_type="era5_land",
                dataset="reanalysis-era5-land",
                variables=["total_precipitation"],
                hours=["00"],
                description="Total precipitation from ERA5-Land (0.1 deg).",
            ),
        ],
    ),
}


def get_peril_config(peril: str) -> PerilConfig:
    key = peril.lower().replace(" ", "_").replace("-", "_")
    if key not in PERIL_CONFIGS:
        raise ValueError(f"Unknown peril '{peril}'. Valid options: {list(PERIL_CONFIGS.keys())}")
    return PERIL_CONFIGS[key]


def list_sources(peril: str) -> List[str]:
    return [s.source_type for s in get_peril_config(peril).sources]


def list_perils() -> List[str]:
    return list(PERIL_CONFIGS.keys())
