from datetime import datetime, timedelta
from enum import Enum


class CheckType(Enum):
    ROW_COUNT = "RcountCheck"
    DUPLICATE = "DuplicateCheck"


def get_param_value(pipeline_name, historical_load_params, cc_list, aod_start_range=1, aod_end_range=1, aod=None):
    _aod = convert_str_to_datetime(aod) if aod else datetime.now()
    aod_start = (_aod - timedelta(aod_start_range)).strftime("%Y-%m-%d")
    aod_end = (_aod - timedelta(aod_end_range)).strftime("%Y-%m-%d")
    aod = _aod.strftime("%Y-%m-%d")
    cc_list = cc_list
    param_dict = {
        "aod_start": aod_start,
        "aod_end": aod_end,
        "cc_list": cc_list,
        "load_type": "daily",
        "aod": aod
    }

    historical_load = eval(historical_load_params) if historical_load_params else None

    if historical_load:
        for _key in historical_load["pipeline_name"]:
            if pipeline_name in _key and _key[pipeline_name].get("execution_time") == datetime.now().strftime("%Y-%m-%d"):
                hist_aod_start = _key[pipeline_name].get("aod_start", aod_start)
                hist_aod_end = _key[pipeline_name].get("aod_end", aod_end)
                hist_aod = _key[pipeline_name].get("aod", aod)
                hist_cc_list = _key[pipeline_name].get("cc_list", cc_list)

                param_dict["aod_start"] = hist_aod_start
                param_dict["aod_end"] = hist_aod_end
                param_dict["aod"] = hist_aod
                param_dict["cc_list"] = hist_cc_list
                param_dict["load_type"] = 'historical'

    return param_dict


def convert_str_to_datetime(datetime_str: str, date_format: str = '%Y-%m-%d') -> datetime:
    return datetime.strptime(datetime_str, date_format)


def daterange(start_date, end_date, step=timedelta(days=1)):
    while start_date <= end_date:
        end_date_temp = (start_date + step - timedelta(days=1))
        if end_date_temp > end_date:
            end_date_temp = end_date
        yield start_date, end_date_temp
        start_date += step


def iterate_str_dates(start_date, end_date):
    """
    Generate string dates between start_date and end_date (inclusive).
    :param start_date: str, in YYYY-MM-DD format
    :param end_date: str, in YYYY-MM-DD format
    :return: generator yielding date strings
    """

    def increment_str_date(date_str):
        year, month, day = map(int, date_str.split("-"))

        # Increment day
        day += 1

        # Handle month-end
        if (day > 31 or
                (month == 2 and day > 28 and (year % 4 != 0 or (year % 100 == 0 and year % 400 != 0))) or
                (month in [4, 6, 9, 11] and day > 30)):
            day = 1
            month += 1

        # Handle year-end
        if month > 12:
            month = 1
            year += 1

        return f"{year:04d}-{month:02d}-{day:02d}"

    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date = increment_str_date(current_date)
