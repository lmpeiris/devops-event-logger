import os


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
