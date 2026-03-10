"""
CHIRPS downloader: downloads CHIRPS v3.0 daily precipitation data from the
Climate Hazards Center (CHC / UCSB), clips to the AOI, and saves one NetCDF
file per year.

Source: https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/rnl/
"""

import calendar
import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import rasterio
import requests
import xarray as xr
from rasterio.windows import from_bounds

from .aoi import BoundingBox


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHIRPS_BASE_URL = "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/rnl/"
MIN_VALID_DAYS = 300          # reject year if fewer valid days than this
WORKERS_PER_YEAR = 4          # parallel day downloads inside one year


# ---------------------------------------------------------------------------
# Downloader class
# ---------------------------------------------------------------------------

class CHIRPSDownloader:
    """
    Downloads CHIRPS v3.0 daily precipitation data for a given AOI and year
    range. Downloads multiple years concurrently (outer parallelism) and
    multiple days per year concurrently (inner parallelism).

    Parameters:
        aoi        : BoundingBox defining the area of interest.
        output_dir : Directory where .nc files are saved.
        max_workers: Max concurrent *year* downloads (default: 10).
    """

    def __init__(
        self,
        aoi: BoundingBox,
        output_dir: str,
        max_workers: int = 10,
    ):
        self.aoi = aoi
        self.output_dir = output_dir
        self.max_workers = max_workers
        self._lock = Lock()
        os.makedirs(output_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _log(self, msg: str) -> None:
        with self._lock:
            print(msg)

    def _download_day(
        self, date_obj: datetime.date
    ) -> Tuple[datetime.date, Optional[np.ndarray], Optional[object]]:
        """Download, clip, and return one day of CHIRPS data."""
        y, m, d = date_obj.year, date_obj.month, date_obj.day
        fname = f"chirps-v3.0.rnl.{y}.{m:02d}.{d:02d}.tif"
        url = f"{CHIRPS_BASE_URL}{y}/{fname}"
        tmp = os.path.join(self.output_dir, f"_tmp_{y}_{m}_{d}.tif")

        try:
            with requests.get(url, stream=True, timeout=30) as r:
                if r.status_code != 200:
                    return date_obj, None, None
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            with rasterio.open(tmp) as src:
                window = from_bounds(
                    self.aoi.minx, self.aoi.miny,
                    self.aoi.maxx, self.aoi.maxy,
                    src.transform,
                )
                data = src.read(1, window=window)
                transform = src.window_transform(window)

            os.remove(tmp)
            return date_obj, data, transform

        except Exception:
            if os.path.exists(tmp):
                os.remove(tmp)
            return date_obj, None, None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def download_year(
        self, year: int
    ) -> Tuple[int, str, float, Optional[str]]:
        """
        Download all daily data for a single year, clip to AOI, and save as
        a compressed NetCDF file.

        Returns:
            (year, status, size_mb, output_path)
            status: 'success' | 'skipped' | 'failed'
        """
        out_file = os.path.join(
            self.output_dir, f"chirps-v3.0.{year}.daily.nc"
        )

        if os.path.exists(out_file):
            self._log(f"[SKIP] CHIRPS {year}")
            return year, "skipped", 0.0, out_file

        n_days = 366 if calendar.isleap(year) else 365
        date_list = [
            datetime.date(year, 1, 1) + datetime.timedelta(days=i)
            for i in range(n_days)
        ]

        # Download days in parallel
        raw: List[Tuple] = []
        with ThreadPoolExecutor(max_workers=WORKERS_PER_YEAR) as ex:
            futures = {ex.submit(self._download_day, d): d for d in date_list}
            done = 0
            for future in as_completed(futures):
                raw.append(future.result())
                done += 1
                if done % 30 == 0:
                    self._log(f"  CHIRPS {year}: {done}/{n_days} days downloaded")

        raw.sort(key=lambda x: x[0])
        valid = [r for r in raw if r[1] is not None]

        if len(valid) < MIN_VALID_DAYS:
            self._log(
                f"[✗] CHIRPS {year}: too much missing data "
                f"({len(valid)}/{n_days} days valid)"
            )
            return year, "failed", 0.0, None

        # Build 3-D array (time × lat × lon)
        ref_shape = valid[0][1].shape
        ref_tf = valid[0][2]
        stack = np.full((n_days, *ref_shape), np.nan, dtype=np.float32)
        idx_map = {d: i for i, d in enumerate(date_list)}
        for d, arr, _ in valid:
            if arr.shape == ref_shape:
                stack[idx_map[d]] = arr

        # Reconstruct coordinate arrays
        h, w = ref_shape
        xs, _ = rasterio.transform.xy(ref_tf, [0] * w, range(w), offset="center")
        _, ys = rasterio.transform.xy(ref_tf, range(h), [0] * h, offset="center")

        ds = xr.Dataset(
            {"precip": (("time", "latitude", "longitude"), stack)},
            coords={
                "time": pd.to_datetime(date_list),
                "latitude": ys,
                "longitude": xs,
            },
            attrs={
                "source": "CHIRPS v3.0",
                "base_url": CHIRPS_BASE_URL,
                "aoi": repr(self.aoi),
                "year": year,
            },
        )
        ds["precip"].attrs = {
            "units": "mm/day",
            "long_name": "Daily precipitation",
            "source": "CHIRPS v3.0",
        }
        ds.to_netcdf(
            out_file,
            encoding={"precip": {"zlib": True, "complevel": 5}},
        )

        size_mb = os.path.getsize(out_file) / 1024 ** 2
        self._log(f"[✓] CHIRPS {year}: {size_mb:.1f} MB → {os.path.basename(out_file)}")
        return year, "success", size_mb, out_file

    def download_period(self, start_year: int, end_year: int) -> Dict:
        """
        Download CHIRPS data for every year in [start_year, end_year].

        Returns a dict with keys:
            files     : sorted list of successfully downloaded/skipped .nc paths
            success   : [(year, path), ...]
            skipped   : [(year, path), ...]
            failed    : [year, ...]
            total_mb  : float
        """
        years = list(range(start_year, end_year + 1))
        self._log(
            f"\nCHIRPS: downloading {start_year}–{end_year} "
            f"({len(years)} years, {self.max_workers} parallel)"
        )

        results: List[Tuple] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(self.download_year, y): y for y in years}
            for future in as_completed(futures):
                results.append(future.result())

        success = [(r[0], r[3]) for r in results if r[1] == "success"]
        skipped = [(r[0], r[3]) for r in results if r[1] == "skipped"]
        failed  = [r[0]         for r in results if r[1] == "failed"]
        total_mb = sum(r[2] for r in results if r[1] == "success")

        self._log(
            f"CHIRPS done: ✓ {len(success)}  → {len(skipped)} skipped  "
            f"✗ {len(failed)} failed  ({total_mb:.1f} MB total)"
        )

        all_files = sorted(p for _, p in success + skipped if p)
        return {
            "files": all_files,
            "success": success,
            "skipped": skipped,
            "failed": failed,
            "total_mb": total_mb,
        }
