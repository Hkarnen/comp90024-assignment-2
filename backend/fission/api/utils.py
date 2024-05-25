import datetime
import calendar

def get_date_limits(year, month=None, day=None, hour=None, minute=None):
    """
    Given year, month, day, hour, and minute, calculate the starting and ending datetime for the given parameters.
    """
    start_date = datetime.datetime(year, 1, 1, 0, 0)
    if month is not None:
        start_date = datetime.datetime(year, month, 1, 0, 0)
        last_day = calendar.monthrange(year, month)[1]
        end_date = datetime.datetime(year, month, last_day, 23, 59, 59)
        if day is not None:
            start_date = datetime.datetime(year, month, day, 0, 0)
            end_date = datetime.datetime(year, month, day, 23, 59, 59)
            if hour is not None:
                start_date = datetime.datetime(year, month, day, hour, 0)
                end_date = datetime.datetime(year, month, day, hour, 59, 59)
                if minute is not None:
                    start_date = datetime.datetime(year, month, day, hour, minute)
                    end_date = datetime.datetime(year, month, day, hour, minute, 59)
    else:
        end_date = datetime.datetime(year, 12, 31, 23, 59, 59)

    return start_date, end_date

    