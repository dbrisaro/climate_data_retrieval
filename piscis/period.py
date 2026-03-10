"""
Historical period computation.

Periods always:
  - Start and end at multiples of 5  (e.g. 1995–2025, 1990–2020)
  - Are 30 years long by default (20 years as fallback)
  - End at the most recent multiple of 5 <= reference year

Examples (reference_year = 2026):
    compute_period()            -> (1995, 2025)
    compute_period(window=20)   -> (2005, 2025)
    compute_period(2024)        -> (1990, 2020)
"""

import datetime
from typing import Optional


def compute_period(
    reference_year: Optional[int] = None,
    window: int = 30,
) -> tuple:
    """
    Compute the most recent historical analysis period.

    Parameters:
        reference_year: Anchor year (defaults to current calendar year).
        window: Length of period in years. Default 30; use 20 as fallback
                when data availability is limited.

    Returns:
        (start_year, end_year) — both are multiples of 5.

    Examples:
        >>> compute_period()            # if today is 2026
        (1995, 2025)
        >>> compute_period(window=20)
        (2005, 2025)
        >>> compute_period(2024)
        (1990, 2020)
        >>> compute_period(2024, window=20)
        (2000, 2020)
    """
    if reference_year is None:
        reference_year = datetime.date.today().year

    # Most recent multiple of 5 that is <= reference_year
    end_year = (reference_year // 5) * 5

    start_year = end_year - window
    return (start_year, end_year)


def get_year_list(start_year: int, end_year: int) -> list:
    """Return list of years in [start_year, end_year] inclusive."""
    return list(range(start_year, end_year + 1))


def describe_period(start_year: int, end_year: int) -> str:
    """Human-readable period description."""
    n = end_year - start_year
    return f"{start_year}–{end_year} ({n}-year period)"
