import datetime


def target_date(date_pattern, today):
    """Get the target date based on the date pattern.

    Args:
        date_pattern (str): Date pattern string (e.g., "yesterday", "month_start").
        today (datetime.datetime): The reference date.

    Returns:
        datetime.datetime: The target date.

    Raises:
        ValueError: If the date_pattern is invalid.
    """
    if date_pattern == "yesterday":
        target_date = today - datetime.timedelta(days=1)
    elif date_pattern == "two_days_ago":
        target_date = today - datetime.timedelta(days=2)
    elif date_pattern == "month_start":
        target_date = today.replace(day=1)
    elif date_pattern == "year_start":
        target_date = today.replace(month=1, day=1)
    else:
        raise ValueError(f"Invalid date_pattern: {date_pattern}")
    return target_date
