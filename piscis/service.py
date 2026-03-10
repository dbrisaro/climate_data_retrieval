"""
DataRequestService: main orchestrator for the Suyana data request pipeline.

Given a peril and an AOI it:
  1. Parses and validates inputs
  2. Computes the historical period (30-year default, multiples of 5)
  3. Looks up the required data sources and variables for the peril
  4. Downloads the data (ERA5 / ERA5-Land / CHIRPS / ...)
  5. Optionally uploads results to S3
  6. Saves a JSON summary alongside the .nc files

Typical usage (called programmatically by Gabriel or from a notebook):

    from piscis import DataRequestService, DataRequest

    result = DataRequestService().run(
        DataRequest(
            lead_id = "lead_042",
            peril   = "drought",
            aoi     = {"maxy": 6.3, "minx": -75.0, "miny": 1.6, "maxx": -69.9},
            # period is auto-computed; pass (start, end) to override
            s3_bucket = "suyana-climate-data",   # omit to skip S3 upload
        )
    )

    print(result.nc_files)   # list of downloaded .nc paths
    print(result.summary)    # full metadata dict

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HOW TO ADD A NEW DATA SOURCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. Create  piscis/<source>_downloader.py  with a class that has:
       __init__(self, aoi: BoundingBox, output_dir: str)
       download_period(self, start_year, end_year) -> dict
           # dict must have key  "files": List[str]  and  "failed": List[int]

2. Add the source_type string to  _dispatch_download()  below
   (look for the clearly marked block).

3. Add the peril (or update an existing one) in  piscis/peril_config.py
   with a SourceConfig that uses your new source_type.

That's it — the rest of the pipeline (S3, summary, result) is automatic.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

import yaml

from .aoi import BoundingBox, parse_aoi
from .era5_downloader import ERA5Downloader
from .chirps_downloader import CHIRPSDownloader
from .peril_config import PerilConfig, get_peril_config
from .period import compute_period, describe_period
from .summary import generate_summary, print_summary, save_summary


# ---------------------------------------------------------------------------
# Request and Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class DataRequest:
    """
    Specification for a single climate data request.

    Attributes:
        lead_id    : Unique identifier for the lead (used for directory naming
                     and the summary file).
        peril      : One of 'drought', 'heatwave', 'cold_spell', 'precipitation'.
        aoi        : Area of interest. Accepted formats:
                       - BoundingBox object
                       - dict  {maxy, miny, minx, maxx}
                       - str   path to a .shp shapefile
        period        : Optional (start_year, end_year). If omitted the service
                        computes the most recent 30-year period that starts at a
                        multiple of 5 (e.g. 1995–2025).
        source_filter : Optional source type to use when a peril has multiple
                        available datasets. Examples:
                          DataRequest(peril="precipitation", source_filter="chirps")
                          DataRequest(peril="precipitation", source_filter="era5_land")
                        If omitted, ALL sources defined for the peril are downloaded.
                        Use `list_sources(peril)` to see what is available.
        output_dir    : Base directory for downloaded files.
                        Files land in  <output_dir>/<lead_id>/<peril>/
        s3_bucket     : If provided, all .nc files are uploaded to this bucket.
        s3_prefix     : S3 key prefix. Defaults to 'climate-data/<lead_id>/<peril>'.
        s3_region     : AWS region for the S3 bucket (default: 'us-east-1').
    """
    lead_id:       str
    peril:         str
    aoi:           Union[BoundingBox, dict, str]
    period:        Optional[tuple] = None
    source_filter: Optional[str]   = None   # e.g. "chirps" or "era5_land"
    output_dir:    str             = "data/requests"
    s3_bucket:     Optional[str]  = None
    s3_prefix:     Optional[str]  = None
    s3_region:     str             = "us-east-1"

    @classmethod
    def from_yaml(cls, path: str) -> "DataRequest":
        """Load a DataRequest from a YAML file."""
        with open(path, "r") as fh:
            cfg = yaml.safe_load(fh)

        period = None
        if "period" in cfg and cfg["period"]:
            p = cfg["period"]
            period = (int(p["start_year"]), int(p["end_year"]))

        return cls(
            lead_id       = str(cfg["lead_id"]),
            peril         = cfg["peril"],
            aoi           = cfg["aoi"],
            period        = period,
            source_filter = cfg.get("source_filter") or None,
            output_dir    = cfg.get("output_dir", "data/requests"),
            s3_bucket     = cfg.get("s3_bucket") or None,
            s3_prefix     = cfg.get("s3_prefix") or None,
            s3_region     = cfg.get("s3_region", "us-east-1"),
        )


@dataclass
class DataRequestResult:
    """
    Result of a completed data request.

    Attributes:
        lead_id      : Same as the input request.
        peril        : Same as the input request.
        period       : (start_year, end_year) that was used.
        aoi          : Parsed BoundingBox.
        nc_files     : List of downloaded .nc file paths.
        s3_uris      : List of S3 URIs (empty if S3 was not requested).
        summary      : Full metadata dict (also saved as JSON).
        summary_path : Path to the saved summary JSON.
        errors       : List of error messages (empty on full success).
    """
    lead_id:      str
    peril:        str
    period:       tuple
    aoi:          BoundingBox
    nc_files:     List[str]         = field(default_factory=list)
    s3_uris:      List[str]         = field(default_factory=list)
    summary:      Dict              = field(default_factory=dict)
    summary_path: Optional[str]     = None
    errors:       List[str]         = field(default_factory=list)

    @property
    def success(self) -> bool:
        return bool(self.nc_files) and not self.errors

    @property
    def partial(self) -> bool:
        return bool(self.nc_files) and bool(self.errors)


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class DataRequestService:
    """
    Orchestrates climate data downloads for the Suyana pipeline.

    Stateless — create a new instance or reuse across multiple requests.
    """

    def run(self, request: DataRequest) -> DataRequestResult:
        """
        Execute a data request end-to-end.

        Parameters:
            request : DataRequest object.

        Returns:
            DataRequestResult with all downloaded file paths, optional S3 URIs,
            and a full metadata summary.
        """
        sep = "═" * 62
        print(f"\n{sep}")
        print(f"  DATA REQUEST SERVICE  |  Lead: {request.lead_id}")
        print(sep)

        # ── 1. Parse AOI ──────────────────────────────────────────────────
        aoi = parse_aoi(request.aoi)
        print(f"  AOI    : {aoi}")

        # ── 2. Resolve period ─────────────────────────────────────────────
        period = request.period or compute_period()
        start_year, end_year = period
        print(f"  Period : {describe_period(start_year, end_year)}")

        # ── 3. Peril config + optional source filter ──────────────────────
        peril_cfg: PerilConfig = get_peril_config(request.peril)
        sources = peril_cfg.sources

        if request.source_filter:
            sources = [s for s in sources if s.source_type == request.source_filter]
            if not sources:
                available = [s.source_type for s in peril_cfg.sources]
                raise ValueError(
                    f"source_filter='{request.source_filter}' not found for peril "
                    f"'{request.peril}'. Available: {available}"
                )

        print(f"  Peril  : {peril_cfg.peril}  —  {peril_cfg.description}")
        print(f"  Sources: {[s.source_type for s in sources]}")

        # ── 4. Output directory ───────────────────────────────────────────
        lead_dir = os.path.join(
            request.output_dir, request.lead_id, request.peril
        )
        os.makedirs(lead_dir, exist_ok=True)
        print(f"  Output : {lead_dir}")

        all_files: List[str] = []
        errors:    List[str] = []

        # ── 5. Download each source ────────────────────────────────────────
        for source in sources:
            print(
                f"\n  ↓ {source.source_type.upper()}  |  "
                f"vars: {source.variables}"
            )
            try:
                res = self._dispatch_download(
                    source, aoi, start_year, end_year, lead_dir
                )
                all_files.extend(res["files"])
                if res.get("failed"):
                    errors.append(
                        f"{source.source_type}: failed years {res['failed']}"
                    )
            except NotImplementedError as exc:
                errors.append(str(exc))
                print(f"  [!] {exc}")
            except Exception as exc:
                msg = f"{source.source_type} download error: {exc}"
                errors.append(msg)
                print(f"  [✗] {msg}")

        # ── 6. S3 upload ──────────────────────────────────────────────────
        s3_uris: List[str] = []
        if request.s3_bucket and all_files:
            from .s3_storage import upload_files
            prefix = (
                request.s3_prefix
                or f"climate-data/{request.lead_id}/{request.peril}"
            )
            print(
                f"\n  ↑ S3  s3://{request.s3_bucket}/{prefix}  "
                f"({len(all_files)} files)"
            )
            try:
                s3_uris = upload_files(
                    local_paths = all_files,
                    bucket      = request.s3_bucket,
                    prefix      = prefix,
                    region      = request.s3_region,
                )
            except Exception as exc:
                msg = f"S3 upload error: {exc}"
                errors.append(msg)
                print(f"  [✗] {msg}")

        # ── 7. Summary ────────────────────────────────────────────────────
        summary = generate_summary(
            lead_id          = request.lead_id,
            peril            = request.peril,
            aoi              = aoi.to_dict(),
            period           = period,
            downloaded_files = all_files,
            s3_uris          = s3_uris,
            errors           = errors,
        )
        summary_path = save_summary(summary, lead_dir)
        print_summary(summary)

        return DataRequestResult(
            lead_id      = request.lead_id,
            peril        = request.peril,
            period       = period,
            aoi          = aoi,
            nc_files     = all_files,
            s3_uris      = s3_uris,
            summary      = summary,
            summary_path = summary_path,
            errors       = errors,
        )

    # ------------------------------------------------------------------
    # Source dispatch  ←  THIS IS WHERE YOU ADD NEW DATA SOURCES
    # ------------------------------------------------------------------

    def _dispatch_download(
        self,
        source,
        aoi: BoundingBox,
        start_year: int,
        end_year: int,
        output_dir: str,
    ) -> dict:
        """
        Route a SourceConfig to the correct downloader and run it.

        Returns a dict with at least:
            "files"  : List[str]   — successfully downloaded .nc paths
            "failed" : List[int]   — years that failed (empty = all good)

        ──────────────────────────────────────────────────────────────
        TO ADD A NEW SOURCE:
          1. Write piscis/<name>_downloader.py  (see chirps_downloader.py
             as a reference)
          2. Import the class at the top of this file
          3. Add an elif block below following the same pattern
        ──────────────────────────────────────────────────────────────
        """

        # ── ERA5 / ERA5-Land (CDS API) ─────────────────────────────────
        if source.source_type in ("era5", "era5_land"):
            dl = ERA5Downloader(aoi=aoi, output_dir=output_dir)
            return dl.download_period(
                dataset    = source.dataset,
                variables  = source.variables,
                start_year = start_year,
                end_year   = end_year,
                hours      = source.hours,
                levels     = source.levels,
            )

        # ── CHIRPS v3.0 (CHC / UCSB) ──────────────────────────────────
        elif source.source_type == "chirps":
            dl = CHIRPSDownloader(aoi=aoi, output_dir=output_dir)
            return dl.download_period(start_year=start_year, end_year=end_year)

        # ── CHIRTS (daily max/min temperature — CHC / UCSB) ───────────
        # TODO: implement CHIRTS support
        #   - Dataset: CHIRTS-daily Tmax / Tmin (~5 km resolution)
        #   - Source:  https://data.chc.ucsb.edu/products/CHIRTSdaily/
        #   - Same download pattern as CHIRPS (one GeoTIFF per day)
        #   - Use for: heatwave / cold_spell as a high-res alternative to ERA5
        #   - Steps:
        #       1. Create piscis/chirts_downloader.py  (copy chirps_downloader.py,
        #          adjust BASE_URL and variable name)
        #       2. Uncomment the elif block below
        #       3. Add source_type="chirts" to heatwave and cold_spell in
        #          piscis/peril_config.py
        # elif source.source_type == "chirts":
        #     from .chirts_downloader import CHIRTSDownloader
        #     dl = CHIRTSDownloader(aoi=aoi, output_dir=output_dir)
        #     return dl.download_period(start_year=start_year, end_year=end_year)

        # ── ERA5-Pressure levels (future) ─────────────────────────────
        # elif source.source_type == "era5_pressure":
        #     dl = ERA5Downloader(aoi=aoi, output_dir=output_dir)
        #     return dl.download_period(
        #         dataset="reanalysis-era5-pressure-levels", ...
        #     )

        # ──────────────────────────────────────────────────────────────
        else:
            raise NotImplementedError(
                f"Source type '{source.source_type}' is not implemented yet. "
                f"See the HOW TO ADD A NEW DATA SOURCE instructions at the "
                f"top of piscis/service.py"
            )

    def run_from_yaml(self, yaml_path: str) -> DataRequestResult:
        """
        Convenience wrapper: load a DataRequest from a YAML file and run it.

        Parameters:
            yaml_path : Path to a request YAML (see configs/request_template.yml).

        Returns:
            DataRequestResult
        """
        request = DataRequest.from_yaml(yaml_path)
        return self.run(request)
