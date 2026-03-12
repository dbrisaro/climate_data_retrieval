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


CHIRPS_BASE_URL = "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/rnl/"
MIN_VALID_DAYS = 300
WORKERS_PER_YEAR = 4


class CHIRPSDownloader:

    def __init__(self, aoi: BoundingBox, output_dir: str, max_workers: int = 10):
        self.aoi = aoi
        self.output_dir = output_dir
        self.max_workers = max_workers
        self._lock = Lock()
        os.makedirs(output_dir, exist_ok=True)

    def _log(self, msg: str) -> None:
        with self._lock:
            print(msg)

    def _download_day(
        self, date_obj: datetime.date
    ) -> Tuple[datetime.date, Optional[np.ndarray], Optional[object]]:
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

    def download_year(self, year: int) -> Tuple[int, str, float, Optional[str]]:
        out_file = os.path.join(self.output_dir, f"chirps-v3.0.{year}.daily.nc")

        if os.path.exists(out_file):
            self._log(f"chirps {year}: skipped")
            return year, "skipped", 0.0, out_file

        n_days = 366 if calendar.isleap(year) else 365
        date_list = [datetime.date(year, 1, 1) + datetime.timedelta(days=i) for i in range(n_days)]

        raw = []
        with ThreadPoolExecutor(max_workers=WORKERS_PER_YEAR) as ex:
            futures = {ex.submit(self._download_day, d): d for d in date_list}
            for future in as_completed(futures):
                raw.append(future.result())

        raw.sort(key=lambda x: x[0])
        valid = [r for r in raw if r[1] is not None]

        if len(valid) < MIN_VALID_DAYS:
            self._log(f"chirps {year}: failed ({len(valid)}/{n_days} days valid)")
            return year, "failed", 0.0, None

        ref_shape = valid[0][1].shape
        ref_tf = valid[0][2]
        stack = np.full((n_days, *ref_shape), np.nan, dtype=np.float32)
        idx_map = {d: i for i, d in enumerate(date_list)}
        for d, arr, _ in valid:
            if arr.shape == ref_shape:
                stack[idx_map[d]] = arr

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
            attrs={"source": "CHIRPS v3.0", "base_url": CHIRPS_BASE_URL, "aoi": repr(self.aoi), "year": year},
        )
        ds["precip"].attrs = {"units": "mm/day", "long_name": "Daily precipitation", "source": "CHIRPS v3.0"}
        ds.to_netcdf(out_file, encoding={"precip": {"zlib": True, "complevel": 5}})

        size_mb = os.path.getsize(out_file) / 1024 ** 2
        self._log(f"chirps {year}: done ({size_mb:.1f} MB)")
        return year, "success", size_mb, out_file

    def download_period(self, start_year: int, end_year: int) -> Dict:
        years = list(range(start_year, end_year + 1))
        print(f"chirps: {start_year}-{end_year} ({len(years)} years)")

        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(self.download_year, y): y for y in years}
            for future in as_completed(futures):
                results.append(future.result())

        success = [(r[0], r[3]) for r in results if r[1] == "success"]
        skipped = [(r[0], r[3]) for r in results if r[1] == "skipped"]
        failed = [r[0] for r in results if r[1] == "failed"]
        total_mb = sum(r[2] for r in results if r[1] == "success")

        print(f"chirps: {len(success)} ok, {len(skipped)} skipped, {len(failed)} failed ({total_mb:.1f} MB)")

        return {
            "files": sorted(p for _, p in success + skipped if p),
            "success": success,
            "skipped": skipped,
            "failed": failed,
            "total_mb": total_mb,
        }
