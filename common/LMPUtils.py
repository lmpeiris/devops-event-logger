import os
import datetime
import pandas as pd


class LMPUtils:
    def __init__(self):
        # nothing much to do here still
        print('[INFO] loaded SMDUtils')

    @classmethod
    def env_bool(cls, env_bool_str: str):
        """read boolean value from enviorn var"""
        bool_value = (os.getenv(env_bool_str, 'False').lower() == 'true')
        print('[DEBUG] ' + env_bool_str + ' is set to ' + str(bool_value))
        return bool_value

    @classmethod
    def rfc2822_to_iso(cls, rfc2822_string: str) -> datetime.datetime:
        """converts Thu, 26 Sep 2024 09:37:22 +0530 like date to 2024-09-26 09:37:22+05:30"""
        iso_datetime = datetime.datetime.strptime(rfc2822_string, '%a, %d %b %Y %H:%M:%S %z')
        return iso_datetime

    @classmethod
    def iso_to_datetime64(cls, series: pd.Series, preserve_timezone: bool = True) -> pd.Series:
        """converts iso8601 compatible string series to datetime64 with or without timezone.
         If not preserve_timezone it will auto-convert to UTC times"""
        if preserve_timezone:
            # user should handle the timezones
            # pandas may use object type to store if mixed tz input, which may also cause issues with pm4py
            converted_series = pd.to_datetime(series, format='ISO8601')
        else:
            # any other method is risky to follow as events are gathered from multiple sources and teams may be global
            # first convert to UTC, this changes times shown if in a different timezone
            converted_series = pd.to_datetime(series, format='ISO8601', utc=True).dt.tz_localize(None)
        return converted_series
