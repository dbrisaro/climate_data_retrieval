"""
Summary generator: creates a structured JSON summary of a completed data
request, plus a human-readable console printout.
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def generate_summary(
    lead_id: str,
    peril: str,
    aoi: dict,
    period: tuple,
    downloaded_files: List[str],
    s3_uris: Optional[List[str]] = None,
    errors: Optional[List[str]] = None,
) -> Dict:
    """
    Build a summary dict for a completed data request.

    Parameters:
        lead_id          : Identifier for the lead / request.
        peril            : Peril type string.
        aoi              : Bounding box dict (maxy, miny, minx, maxx).
        period           : (start_year, end_year) tuple.
        downloaded_files : List of local .nc file paths.
        s3_uris          : Optional list of S3 URIs.
        errors           : Optional list of error messages.

    Returns:
        Summary dict.
    """
    start_year, end_year = period

    file_details: List[Dict] = []
    total_size_mb = 0.0
    for f in downloaded_files:
        if f and os.path.exists(f):
            size_mb = os.path.getsize(f) / 1024 ** 2
            total_size_mb += size_mb
            file_details.append(
                {
                    "filename": os.path.basename(f),
                    "path":     f,
                    "size_mb":  round(size_mb, 2),
                }
            )

    errors_clean = errors or []
    status = "success" if not errors_clean else (
        "partial" if file_details else "failed"
    )

    return {
        "lead_id":      lead_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status":       status,
        "request": {
            "peril": peril,
            "aoi":   aoi,
            "period": {
                "start_year": start_year,
                "end_year":   end_year,
                "n_years":    end_year - start_year + 1,
            },
        },
        "output": {
            "n_files":      len(file_details),
            "total_size_mb": round(total_size_mb, 2),
            "files":        file_details,
            "s3_uris":      s3_uris or [],
        },
        "errors": errors_clean,
    }


def save_summary(summary: Dict, output_dir: str) -> str:
    """
    Save the summary dict as a JSON file.

    Returns the path to the saved file.
    """
    os.makedirs(output_dir, exist_ok=True)
    lead_id   = summary.get("lead_id", "request")
    ts        = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename  = f"summary_{lead_id}_{ts}.json"
    path      = os.path.join(output_dir, filename)

    with open(path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=False)

    print(f"[Summary] Saved → {path}")
    return path


def print_summary(summary: Dict) -> None:
    """Print a human-readable summary to stdout."""
    req    = summary["request"]
    out    = summary["output"]
    period = req["period"]
    aoi    = req["aoi"]

    sep = "─" * 60
    print(f"\n{sep}")
    print(f"  DATA REQUEST SUMMARY  |  Lead: {summary['lead_id']}")
    print(sep)
    print(f"  Peril    : {req['peril']}")
    print(
        f"  Period   : {period['start_year']}–{period['end_year']} "
        f"({period['n_years']} years)"
    )
    print(
        f"  AOI      : N={aoi['maxy']}  S={aoi['miny']}  "
        f"W={aoi['minx']}  E={aoi['maxx']}"
    )
    print(f"  Files    : {out['n_files']} files  /  {out['total_size_mb']:.1f} MB total")
    if out["s3_uris"]:
        print(f"  S3       : {len(out['s3_uris'])} files uploaded")
    if summary["errors"]:
        print(f"  Errors   : {len(summary['errors'])}")
        for e in summary["errors"]:
            print(f"             • {e}")
    print(f"  Status   : {summary['status'].upper()}")
    print(f"{sep}\n")
