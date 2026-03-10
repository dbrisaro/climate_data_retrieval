"""AOI (Area of Interest) handling: accepts bounding boxes or shapefiles."""

import os
from dataclasses import dataclass
from typing import Union


@dataclass
class BoundingBox:
    """
    Bounding box for an area of interest (WGS84 / EPSG:4326).

    Attributes:
        maxy: North latitude
        miny: South latitude
        minx: West longitude
        maxx: East longitude
    """

    maxy: float  # North
    miny: float  # South
    minx: float  # West
    maxx: float  # East

    def to_era5_area(self) -> list:
        """Return [North, West, South, East] as required by the CDS API."""
        return [self.maxy, self.minx, self.miny, self.maxx]

    def to_dict(self) -> dict:
        return {
            "maxy": self.maxy,
            "miny": self.miny,
            "minx": self.minx,
            "maxx": self.maxx,
        }

    def __repr__(self) -> str:
        return (
            f"BoundingBox(N={self.maxy}, S={self.miny}, "
            f"W={self.minx}, E={self.maxx})"
        )


def aoi_from_dict(d: dict) -> BoundingBox:
    """
    Create a BoundingBox from a dictionary.

    Expected keys: maxy (North), miny (South), minx (West), maxx (East).
    """
    required = {"maxy", "miny", "minx", "maxx"}
    missing = required - set(d.keys())
    if missing:
        raise ValueError(f"AOI dict is missing keys: {missing}")
    return BoundingBox(
        maxy=float(d["maxy"]),
        miny=float(d["miny"]),
        minx=float(d["minx"]),
        maxx=float(d["maxx"]),
    )


def aoi_from_shapefile(path: str) -> BoundingBox:
    """
    Extract the bounding box of a shapefile (reprojects to WGS84 if needed).

    Parameters:
        path: Path to the .shp file

    Returns:
        BoundingBox with the total bounds of all features
    """
    try:
        import geopandas as gpd
    except ImportError:
        raise ImportError(
            "geopandas is required for shapefile support: pip install geopandas"
        )

    if not os.path.exists(path):
        raise FileNotFoundError(f"Shapefile not found: {path}")

    gdf = gpd.read_file(path)

    # Reproject to WGS84 if needed
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    minx, miny, maxx, maxy = gdf.total_bounds
    return BoundingBox(maxy=float(maxy), miny=float(miny), minx=float(minx), maxx=float(maxx))


def parse_aoi(aoi: Union[BoundingBox, dict, str]) -> BoundingBox:
    """
    Parse an AOI from multiple input formats.

    Parameters:
        aoi: One of:
            - BoundingBox object (returned as-is)
            - dict with keys maxy, miny, minx, maxx
            - str path to a shapefile (.shp)

    Returns:
        BoundingBox
    """
    if isinstance(aoi, BoundingBox):
        return aoi
    elif isinstance(aoi, dict):
        return aoi_from_dict(aoi)
    elif isinstance(aoi, str):
        return aoi_from_shapefile(aoi)
    else:
        raise ValueError(
            f"Cannot parse AOI from type {type(aoi)}. "
            "Expected a BoundingBox, dict, or path to a shapefile."
        )
