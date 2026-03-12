import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional


def generate_summary(
    lead_id: str,
    peril: str,
    aoi: dict,
    period: tuple,
    downloaded_files: List[str],
    s3_uris: Optional[List[str]] = None,
    errors: Optional[List[str]] = None,
) -> Dict:
    start_year, end_year = period

    file_details = []
    total_size_mb = 0.0
    for f in downloaded_files:
        if f and os.path.exists(f):
            size_mb = os.path.getsize(f) / 1024 ** 2
            total_size_mb += size_mb
            file_details.append({
                "filename": os.path.basename(f),
                "path": f,
                "size_mb": round(size_mb, 2),
            })

    errors_clean = errors or []
    status = "success" if not errors_clean else ("partial" if file_details else "failed")

    return {
        "lead_id": lead_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "request": {
            "peril": peril,
            "aoi": aoi,
            "period": {
                "start_year": start_year,
                "end_year": end_year,
                "n_years": end_year - start_year + 1,
            },
        },
        "output": {
            "n_files": len(file_details),
            "total_size_mb": round(total_size_mb, 2),
            "files": file_details,
            "s3_uris": s3_uris or [],
        },
        "errors": errors_clean,
    }


def save_summary(summary: Dict, output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"summary_{summary.get('lead_id', 'request')}_{ts}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=False)
    return path


def print_summary(summary: Dict) -> None:
    req = summary["request"]
    out = summary["output"]
    p = req["period"]
    aoi = req["aoi"]
    print(f"\nSummary | lead: {summary['lead_id']} | peril: {req['peril']}")
    print(f"  period : {p['start_year']}-{p['end_year']} ({p['n_years']} years)")
    print(f"  aoi    : N={aoi['maxy']} S={aoi['miny']} W={aoi['minx']} E={aoi['maxx']}")
    print(f"  files  : {out['n_files']} ({out['total_size_mb']:.1f} MB)")
    if out["s3_uris"]:
        print(f"  s3     : {len(out['s3_uris'])} files uploaded")
    if summary["errors"]:
        for e in summary["errors"]:
            print(f"  error  : {e}")
    print(f"  status : {summary['status']}\n")
