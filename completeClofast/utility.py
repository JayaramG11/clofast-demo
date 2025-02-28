from datetime import datetime

def generate_cron_expression(date_str: str, frequency: str = "daily") -> str:
    """
    Generate a cron expression from a given ISO 8601 date string and frequency.

    Args:
        date_str (str): The ISO 8601 formatted date string (e.g., "2025-02-28T14:30:00Z").
        frequency (str): The schedule frequency ("daily", "weekly", "monthly", "hourly").
                         Defaults to "daily".

    Returns:
        str: A cron expression string.

    Raises:
        ValueError: If the frequency is invalid or the date format is incorrect.
    """
    try:
        # Convert ISO 8601 string to datetime object
        dt = datetime.fromisoformat(date_str)
    except ValueError:
        raise ValueError("Invalid date format. Use ISO 8601 format: 'YYYY-MM-DDTHH:MM:SSZ'.")

    minute = dt.minute
    hour = dt.hour
    day = dt.day
    month = dt.month
    weekday = dt.weekday()  # 0 = Monday, 6 = Sunday

    # Generate cron expression based on frequency
    if frequency == "daily":
        cron_expr = f"{minute} {hour} * * *"
    elif frequency == "weekly":
        cron_expr = f"{minute} {hour} * * {weekday}"
    elif frequency == "monthly":
        cron_expr = f"{minute} {hour} {day} * *"
    elif frequency == "intraday":
        cron_expr = f"{minute} * * * *"
    else:
        raise ValueError("Invalid frequency. Choose from 'daily', 'weekly', 'monthly', or 'hourly'.")

    return cron_expr
