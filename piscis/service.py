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


@dataclass
class DataRequest:
    lead_id: str
    peril: str
    aoi: Union[BoundingBox, dict, str]
    period: Optional[tuple] = None
    source_filter: Optional[str] = None
    output_dir: str = "data/requests"
    s3_bucket: Optional[str] = None
    s3_prefix: Optional[str] = None
    s3_region: str = "us-east-1"

    @classmethod
    def from_yaml(cls, path: str) -> "DataRequest":
        with open(path, "r") as fh:
            cfg = yaml.safe_load(fh)
        period = None
        if cfg.get("period"):
            p = cfg["period"]
            period = (int(p["start_year"]), int(p["end_year"]))
        return cls(
            lead_id=str(cfg["lead_id"]),
            peril=cfg["peril"],
            aoi=cfg["aoi"],
            period=period,
            source_filter=cfg.get("source_filter") or None,
            output_dir=cfg.get("output_dir", "data/requests"),
            s3_bucket=cfg.get("s3_bucket") or None,
            s3_prefix=cfg.get("s3_prefix") or None,
            s3_region=cfg.get("s3_region", "us-east-1"),
        )


@dataclass
class DataRequestResult:
    lead_id: str
    peril: str
    period: tuple
    aoi: BoundingBox
    nc_files: List[str] = field(default_factory=list)
    s3_uris: List[str] = field(default_factory=list)
    summary: Dict = field(default_factory=dict)
    summary_path: Optional[str] = None
    errors: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return bool(self.nc_files) and not self.errors

    @property
    def partial(self) -> bool:
        return bool(self.nc_files) and bool(self.errors)


class DataRequestService:

    def run(self, request: DataRequest) -> DataRequestResult:
        aoi = parse_aoi(request.aoi)
        period = request.period or compute_period()
        start_year, end_year = period

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

        lead_dir = os.path.join(request.output_dir, request.lead_id, request.peril)
        os.makedirs(lead_dir, exist_ok=True)

        print(f"lead: {request.lead_id} | peril: {request.peril} | period: {describe_period(start_year, end_year)}")

        all_files: List[str] = []
        errors: List[str] = []

        for source in sources:
            try:
                res = self._dispatch_download(source, aoi, start_year, end_year, lead_dir)
                all_files.extend(res["files"])
                if res.get("failed"):
                    errors.append(f"{source.source_type}: failed years {res['failed']}")
            except NotImplementedError as exc:
                errors.append(str(exc))
                print(f"not implemented: {exc}")
            except Exception as exc:
                msg = f"{source.source_type} download error: {exc}"
                errors.append(msg)
                print(msg)

        s3_uris: List[str] = []
        if request.s3_bucket and all_files:
            from .s3_storage import upload_files
            prefix = request.s3_prefix or f"climate-data/{request.lead_id}/{request.peril}"
            try:
                s3_uris = upload_files(
                    local_paths=all_files,
                    bucket=request.s3_bucket,
                    prefix=prefix,
                    region=request.s3_region,
                )
            except Exception as exc:
                msg = f"s3 upload error: {exc}"
                errors.append(msg)
                print(msg)

        summary = generate_summary(
            lead_id=request.lead_id,
            peril=request.peril,
            aoi=aoi.to_dict(),
            period=period,
            downloaded_files=all_files,
            s3_uris=s3_uris,
            errors=errors,
        )
        summary_path = save_summary(summary, lead_dir)
        print_summary(summary)

        return DataRequestResult(
            lead_id=request.lead_id,
            peril=request.peril,
            period=period,
            aoi=aoi,
            nc_files=all_files,
            s3_uris=s3_uris,
            summary=summary,
            summary_path=summary_path,
            errors=errors,
        )

    def _dispatch_download(self, source, aoi: BoundingBox, start_year: int, end_year: int, output_dir: str) -> dict:
        if source.source_type in ("era5", "era5_land"):
            dl = ERA5Downloader(aoi=aoi, output_dir=output_dir)
            return dl.download_period(
                dataset=source.dataset,
                variables=source.variables,
                start_year=start_year,
                end_year=end_year,
                hours=source.hours,
                levels=source.levels,
            )

        elif source.source_type == "chirps":
            dl = CHIRPSDownloader(aoi=aoi, output_dir=output_dir)
            return dl.download_period(start_year=start_year, end_year=end_year)

        # TODO: implement CHIRTS support
        #   Dataset: CHIRTS-daily Tmax/Tmin (~5 km), https://data.chc.ucsb.edu/products/CHIRTSdaily/
        #   Same download pattern as CHIRPS (one GeoTIFF per day).
        #   Steps:
        #     1. Create piscis/chirts_downloader.py (copy chirps_downloader.py, adjust BASE_URL and variable name)
        #     2. Uncomment the elif block below
        #     3. Add source_type="chirts" to heatwave and cold_spell in piscis/peril_config.py
        # elif source.source_type == "chirts":
        #     from .chirts_downloader import CHIRTSDownloader
        #     dl = CHIRTSDownloader(aoi=aoi, output_dir=output_dir)
        #     return dl.download_period(start_year=start_year, end_year=end_year)

        else:
            raise NotImplementedError(
                f"Source type '{source.source_type}' is not implemented. "
                f"See piscis/service.py _dispatch_download() to add it."
            )

    def run_from_yaml(self, yaml_path: str) -> DataRequestResult:
        return self.run(DataRequest.from_yaml(yaml_path))
