import datetime
from typing import Optional


def compute_period(reference_year: Optional[int] = None, window: int = 30) -> tuple:
    if reference_year is None:
        reference_year = datetime.date.today().year
    end_year = (reference_year // 5) * 5
    start_year = end_year - window
    return (start_year, end_year)


def get_year_list(start_year: int, end_year: int) -> list:
    return list(range(start_year, end_year + 1))


def describe_period(start_year: int, end_year: int) -> str:
    return f"{start_year}-{end_year} ({end_year - start_year}-year period)"
