import os
from dataclasses import dataclass
from typing import Union


@dataclass
class BoundingBox:
    maxy: float
    miny: float
    minx: float
    maxx: float

    def to_era5_area(self) -> list:
        return [self.maxy, self.minx, self.miny, self.maxx]

    def to_dict(self) -> dict:
        return {"maxy": self.maxy, "miny": self.miny, "minx": self.minx, "maxx": self.maxx}

    def __repr__(self) -> str:
        return f"BoundingBox(N={self.maxy}, S={self.miny}, W={self.minx}, E={self.maxx})"


def aoi_from_dict(d: dict) -> BoundingBox:
    missing = {"maxy", "miny", "minx", "maxx"} - set(d.keys())
    if missing:
        raise ValueError(f"AOI dict is missing keys: {missing}")
    return BoundingBox(
        maxy=float(d["maxy"]),
        miny=float(d["miny"]),
        minx=float(d["minx"]),
        maxx=float(d["maxx"]),
    )


def aoi_from_shapefile(path: str) -> BoundingBox:
    try:
        import geopandas as gpd
    except ImportError:
        raise ImportError("geopandas is required for shapefile support: pip install geopandas")

    if not os.path.exists(path):
        raise FileNotFoundError(f"Shapefile not found: {path}")

    gdf = gpd.read_file(path)
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    minx, miny, maxx, maxy = gdf.total_bounds
    return BoundingBox(maxy=float(maxy), miny=float(miny), minx=float(minx), maxx=float(maxx))


def parse_aoi(aoi: Union[BoundingBox, dict, str]) -> BoundingBox:
    if isinstance(aoi, BoundingBox):
        return aoi
    elif isinstance(aoi, dict):
        return aoi_from_dict(aoi)
    elif isinstance(aoi, str):
        return aoi_from_shapefile(aoi)
    else:
        raise ValueError(f"Cannot parse AOI from type {type(aoi)}. Expected BoundingBox, dict, or shapefile path.")
