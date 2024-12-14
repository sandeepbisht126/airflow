from datetime import datetime, timedelta


def get_param_value(pipeline_name=None, historical_load_params=None, cc_list=None, aod_start_range=1, aod_end_range=1):
    aod_start = (datetime.now() - timedelta(aod_start_range)).strftime("%Y-%m-%d")
    aod_end = (datetime.now() - timedelta(aod_end_range)).strftime("%Y-%m-%d")
    aod = datetime.now().strftime("%Y-%m-%d")
    cc_list = cc_list
    param_dict = {
        "aod_start": aod_start,
        "aod_end": aod_end,
        "aod": aod,
        "cc_list": cc_list,
        "hist_flag": False
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
                param_dict["hist_flag"] = True

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
