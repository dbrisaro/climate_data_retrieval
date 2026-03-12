import os
from typing import Dict, List, Optional, Tuple

from .aoi import BoundingBox
from .downloader import download_data


_MONTHS = [f"{m:02d}" for m in range(1, 13)]
_DAYS = [f"{d:02d}" for d in range(1, 32)]

_DATASET_SHORTNAMES = {
    "reanalysis-era5-single-levels": "era5",
    "reanalysis-era5-land": "era5land",
    "reanalysis-era5-pressure-levels": "era5pl",
}


def _shortname(dataset: str) -> str:
    return _DATASET_SHORTNAMES.get(dataset, dataset.replace("reanalysis-", ""))


def _normalise_hours(hours: List[str]) -> List[str]:
    return [f"{h}:00" if ":" not in h else h for h in hours]


class ERA5Downloader:

    def __init__(self, aoi: BoundingBox, output_dir: str):
        self.aoi = aoi
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def _build_params(self, variables, year, hours, levels=None) -> dict:
        params = {
            "product_type": "reanalysis",
            "variable": variables,
            "year": str(year),
            "month": _MONTHS,
            "day": _DAYS,
            "time": _normalise_hours(hours),
            "area": self.aoi.to_era5_area(),
            "format": "netcdf",
        }
        if levels:
            params["pressure_level"] = levels
        return params

    def _output_path(self, dataset: str, variables: List[str], year: int) -> str:
        short = _shortname(dataset)
        vartag = "_".join(v[:10] for v in variables[:2])
        return os.path.join(self.output_dir, f"{short}.{year}.{vartag}.nc")

    def download_year(
        self,
        dataset: str,
        variables: List[str],
        year: int,
        hours: List[str],
        levels: Optional[List[str]] = None,
    ) -> Tuple[int, str, float, Optional[str]]:
        out_file = self._output_path(dataset, variables, year)
        short = _shortname(dataset)

        if os.path.exists(out_file):
            print(f"{short} {year}: skipped")
            return year, "skipped", 0.0, out_file

        params = self._build_params(variables, year, hours, levels)
        try:
            download_data(dataset, params, out_file)
            size_mb = os.path.getsize(out_file) / 1024 ** 2
            print(f"{short} {year}: done ({size_mb:.1f} MB)")
            return year, "success", size_mb, out_file
        except Exception as exc:
            print(f"{short} {year}: failed - {exc}")
            if os.path.exists(out_file):
                os.remove(out_file)
            return year, "failed", 0.0, None

    def download_period(
        self,
        dataset: str,
        variables: List[str],
        start_year: int,
        end_year: int,
        hours: List[str],
        levels: Optional[List[str]] = None,
    ) -> Dict:
        years = list(range(start_year, end_year + 1))
        short = _shortname(dataset)
        print(f"{short}: {start_year}-{end_year} ({len(years)} years)")

        success, skipped, failed = [], [], []

        for year in years:
            yr, status, _, path = self.download_year(dataset, variables, year, hours, levels)
            if status == "success":
                success.append((yr, path))
            elif status == "skipped":
                skipped.append((yr, path))
            else:
                failed.append(yr)

        total_mb = sum(
            os.path.getsize(p) / 1024 ** 2
            for _, p in success + skipped
            if p and os.path.exists(p)
        )

        print(f"{short}: {len(success)} ok, {len(skipped)} skipped, {len(failed)} failed ({total_mb:.1f} MB)")

        return {
            "files": sorted(p for _, p in success + skipped if p),
            "success": success,
            "skipped": skipped,
            "failed": failed,
            "total_mb": total_mb,
        }
