from datetime import datetime


def format_sf_timestamp(time_str):
    return datetime.strptime(time_str, "%Y-%m-%d").strftime("%m/%d/%Y")


def time_string():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
