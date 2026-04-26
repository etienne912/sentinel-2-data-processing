import argparse
from datetime import datetime


def parse_date(date_str: str) -> datetime:
    """
    Parse a date string into a datetime object.

    Parameters
    ----------
    date_str : string
        Date string to parse

    Returns
    -------
    datetime
        Datetime object to parse

    Raises
    ------
    ValueError
        Raised if date string is invalid
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Expected YYYY-MM-DD.")
