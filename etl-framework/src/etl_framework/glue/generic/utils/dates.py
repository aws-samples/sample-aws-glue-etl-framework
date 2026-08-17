"""
Date Utilities.

Provides date calculation functions used by readers and writers for
date-based partitioning, filtering, and path generation.
"""

from datetime import datetime, timedelta
from typing import Optional

from etl_framework.glue.generic.models.job_config_models import DateParameters


def compute_date_parameters(
    run_datetime: Optional[datetime] = None,
) -> DateParameters:
    """
    Compute all date parameters from a given datetime.

    If no datetime is provided, uses the current UTC time.

    Args:
        run_datetime: The reference datetime (defaults to datetime.utcnow())

    Returns:
        DateParameters with all computed date fields
    """
    if run_datetime is None:
        run_datetime = datetime.utcnow()

    return DateParameters(
        run_date=run_datetime.strftime("%Y-%m-%d"),
        run_hour=run_datetime.strftime("%H"),
        run_datetime=run_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        path_date_format=run_datetime.strftime("%Y/%m/%d"),
        prev_month_end=get_prev_month_end(run_datetime).strftime("%Y-%m-%d"),
        prev_quarter_end=get_prev_quarter_end(run_datetime).strftime("%Y-%m-%d"),
        year=run_datetime.strftime("%Y"),
        month=run_datetime.strftime("%m"),
        day=run_datetime.strftime("%d"),
    )


def get_prev_month_end(reference_date: Optional[datetime] = None) -> datetime:
    """
    Get the last day of the previous month.

    Args:
        reference_date: The date to calculate from (defaults to today)

    Returns:
        datetime representing the last day of the previous month
    """
    if reference_date is None:
        reference_date = datetime.utcnow()

    # First day of current month, minus one day = last day of prev month
    first_of_month = reference_date.replace(day=1)
    return first_of_month - timedelta(days=1)


def get_prev_quarter_end(reference_date: Optional[datetime] = None) -> datetime:
    """
    Get the last day of the previous quarter.

    Quarters: Q1 (Jan-Mar), Q2 (Apr-Jun), Q3 (Jul-Sep), Q4 (Oct-Dec)

    Args:
        reference_date: The date to calculate from (defaults to today)

    Returns:
        datetime representing the last day of the previous quarter
    """
    if reference_date is None:
        reference_date = datetime.utcnow()

    # Determine current quarter
    month = reference_date.month
    if month <= 3:
        # Current Q1 -> prev quarter end is Dec 31 of previous year
        return datetime(reference_date.year - 1, 12, 31)
    elif month <= 6:
        # Current Q2 -> prev quarter end is Mar 31
        return datetime(reference_date.year, 3, 31)
    elif month <= 9:
        # Current Q3 -> prev quarter end is Jun 30
        return datetime(reference_date.year, 6, 30)
    else:
        # Current Q4 -> prev quarter end is Sep 30
        return datetime(reference_date.year, 9, 30)


def get_date_range_paths(
    start_date: str,
    end_date: str,
    date_format: str = "%Y-%m-%d",
    path_format: str = "%Y/%m/%d",
) -> list:
    """
    Generate a list of date-based path segments between two dates.

    Useful for generating S3 path prefixes for date-partitioned data.

    Args:
        start_date: Start date string
        end_date: End date string
        date_format: Format of the input date strings
        path_format: Format for the output path segments

    Returns:
        List of date path strings
    """
    start = datetime.strptime(start_date, date_format)
    end = datetime.strptime(end_date, date_format)

    paths = []
    current = start
    while current <= end:
        paths.append(current.strftime(path_format))
        current += timedelta(days=1)

    return paths
