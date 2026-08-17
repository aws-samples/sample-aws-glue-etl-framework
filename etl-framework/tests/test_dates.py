"""Tests for date utilities."""

from datetime import datetime

from etl_framework.glue.generic.utils.dates import (
    compute_date_parameters,
    get_date_range_paths,
    get_prev_month_end,
    get_prev_quarter_end,
)


class TestComputeDateParameters:
    """Tests for compute_date_parameters."""

    def test_basic_computation(self):
        """Test date parameter computation."""
        dt = datetime(2024, 3, 15, 10, 30, 45)
        params = compute_date_parameters(dt)

        assert params.run_date == "2024-03-15"
        assert params.run_hour == "10"
        assert params.run_datetime == "2024-03-15 10:30:45"
        assert params.path_date_format == "2024/03/15"
        assert params.year == "2024"
        assert params.month == "03"
        assert params.day == "15"

    def test_default_uses_current_time(self):
        """Test that None defaults to current time."""
        params = compute_date_parameters()
        assert params.run_date is not None
        assert len(params.run_date) == 10


class TestGetPrevMonthEnd:
    """Tests for get_prev_month_end."""

    def test_march_gives_feb_end(self):
        """Test that March reference gives Feb 28/29."""
        result = get_prev_month_end(datetime(2024, 3, 15))
        assert result.month == 2
        assert result.day == 29  # 2024 is leap year

    def test_jan_gives_dec_end(self):
        """Test that January gives Dec 31 of previous year."""
        result = get_prev_month_end(datetime(2024, 1, 10))
        assert result.year == 2023
        assert result.month == 12
        assert result.day == 31

    def test_non_leap_year(self):
        """Test Feb end in non-leap year."""
        result = get_prev_month_end(datetime(2023, 3, 1))
        assert result.month == 2
        assert result.day == 28


class TestGetPrevQuarterEnd:
    """Tests for get_prev_quarter_end."""

    def test_q1_gives_dec_31(self):
        """Test Q1 reference gives Dec 31 of previous year."""
        result = get_prev_quarter_end(datetime(2024, 2, 15))
        assert result == datetime(2023, 12, 31)

    def test_q2_gives_mar_31(self):
        """Test Q2 reference gives Mar 31."""
        result = get_prev_quarter_end(datetime(2024, 5, 1))
        assert result == datetime(2024, 3, 31)

    def test_q3_gives_jun_30(self):
        """Test Q3 reference gives Jun 30."""
        result = get_prev_quarter_end(datetime(2024, 8, 20))
        assert result == datetime(2024, 6, 30)

    def test_q4_gives_sep_30(self):
        """Test Q4 reference gives Sep 30."""
        result = get_prev_quarter_end(datetime(2024, 11, 1))
        assert result == datetime(2024, 9, 30)


class TestGetDateRangePaths:
    """Tests for get_date_range_paths."""

    def test_single_day(self):
        """Test single day range."""
        paths = get_date_range_paths("2024-01-01", "2024-01-01")
        assert paths == ["2024/01/01"]

    def test_three_day_range(self):
        """Test three day range."""
        paths = get_date_range_paths("2024-01-01", "2024-01-03")
        assert paths == ["2024/01/01", "2024/01/02", "2024/01/03"]

    def test_cross_month_boundary(self):
        """Test range crossing month boundary."""
        paths = get_date_range_paths("2024-01-30", "2024-02-01")
        assert paths == ["2024/01/30", "2024/01/31", "2024/02/01"]

    def test_custom_format(self):
        """Test custom path format."""
        paths = get_date_range_paths(
            "2024-01-01", "2024-01-02", path_format="year=%Y/month=%m/day=%d"
        )
        assert paths == ["year=2024/month=01/day=01", "year=2024/month=01/day=02"]
