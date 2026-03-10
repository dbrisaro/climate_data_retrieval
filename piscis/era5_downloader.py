"""
ERA5 / ERA5-Land downloader: wraps the CDS API for structured period downloads.

Downloads one year per CDS request (all months, all days, selected hours).
Requests are issued sequentially because the CDS queue is shared and
submitting many requests simultaneously just creates a backlog.
"""

import os
from typing import Dict, List, Optional, Tuple

from .aoi import BoundingBox
from .downloader import download_data   # existing CDS wrapper


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MONTHS = [f"{m:02d}" for m in range(1, 13)]
_DAYS   = [f"{d:02d}" for d in range(1, 32)]

_DATASET_SHORTNAMES = {
    "reanalysis-era5-single-levels": "era5",
    "reanalysis-era5-land":          "era5land",
    "reanalysis-era5-pressure-levels": "era5pl",
}


def _shortname(dataset: str) -> str:
    return _DATASET_SHORTNAMES.get(dataset, dataset.replace("reanalysis-", ""))


def _normalise_hours(hours: List[str]) -> List[str]:
    """Ensure hours are in 'HH:00' format expected by CDS."""
    return [f"{h}:00" if ":" not in h else h for h in hours]


# ---------------------------------------------------------------------------
# Downloader class
# ---------------------------------------------------------------------------

class ERA5Downloader:
    """
    Downloads ERA5 or ERA5-Land data for a given AOI and year range.

    Parameters:
        aoi        : BoundingBox for the area of interest.
        output_dir : Directory where .nc files are saved.
    """

    def __init__(self, aoi: BoundingBox, output_dir: str):
        self.aoi = aoi
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_params(
        self,
        variables: List[str],
        year: int,
        hours: List[str],
        levels: Optional[List[str]] = None,
    ) -> dict:
        """Build CDS API request parameters for one year."""
        params: dict = {
            "product_type": "reanalysis",
            "variable":     variables,
            "year":         str(year),
            "month":        _MONTHS,
            "day":          _DAYS,
            "time":         _normalise_hours(hours),
            "area":         self.aoi.to_era5_area(),  # [N, W, S, E]
            "format":       "netcdf",
        }
        if levels:
            params["pressure_level"] = levels
        return params

    def _output_path(
        self,
        dataset: str,
        variables: List[str],
        year: int,
    ) -> str:
        """Build a deterministic filename for a given request."""
        short  = _shortname(dataset)
        # Use first two variable names (truncated) as a readable tag
        vartag = "_".join(v[:10] for v in variables[:2])
        return os.path.join(self.output_dir, f"{short}.{year}.{vartag}.nc")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def download_year(
        self,
        dataset: str,
        variables: List[str],
        year: int,
        hours: List[str],
        levels: Optional[List[str]] = None,
    ) -> Tuple[int, str, float, Optional[str]]:
        """
        Download one year of ERA5 data via the CDS API.

        Returns:
            (year, status, size_mb, output_path)
            status: 'success' | 'skipped' | 'failed'
        """
        out_file = self._output_path(dataset, variables, year)
        short = _shortname(dataset)

        if os.path.exists(out_file):
            print(f"[SKIP] {short} {year}")
            return year, "skipped", 0.0, out_file

        params = self._build_params(variables, year, hours, levels)
        try:
            download_data(dataset, params, out_file)
            size_mb = os.path.getsize(out_file) / 1024 ** 2
            print(f"[✓] {short} {year}: {size_mb:.1f} MB → {os.path.basename(out_file)}")
            return year, "success", size_mb, out_file
        except Exception as exc:
            print(f"[✗] {short} {year}: {exc}")
            # Remove partial file if it was created
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
        """
        Download ERA5 data for every year in [start_year, end_year].

        CDS requests are sequential to respect queue limits.

        Returns a dict with keys:
            files     : sorted list of .nc paths (success + skipped)
            success   : [(year, path), ...]
            skipped   : [(year, path), ...]
            failed    : [year, ...]
            total_mb  : float
        """
        years = list(range(start_year, end_year + 1))
        short = _shortname(dataset)
        print(
            f"\n{short}: downloading {start_year}–{end_year} "
            f"({len(years)} years, sequential CDS requests)"
        )

        success: List[Tuple] = []
        skipped: List[Tuple] = []
        failed:  List[int]   = []

        for year in years:
            yr, status, _, path = self.download_year(
                dataset, variables, year, hours, levels
            )
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

        print(
            f"{short} done: ✓ {len(success)}  → {len(skipped)} skipped  "
            f"✗ {len(failed)} failed  ({total_mb:.1f} MB total)"
        )

        all_files = sorted(p for _, p in success + skipped if p)
        return {
            "files":    all_files,
            "success":  success,
            "skipped":  skipped,
            "failed":   failed,
            "total_mb": total_mb,
        }
