import logging


class LMPLogger:
    def __init__(self, init_prefix: str, logger: logging.Logger):
        self.logger = logger
        self.init_prefix = '[' + init_prefix + ']'
        self.prefix = str(self.init_prefix)
        # calling instance method from init - just to test
        self.info('LMPLogger v0.0.1 initialised')
        # TODO: use record factory for better logging
        #  https://stackoverflow.com/questions/17558552/how-do-i-add-custom-field-to-python-log-format-string

    def set_prefix(self, arg_list: list[str]):
        """Set all arguments as a list"""
        prefix = ''
        for item in arg_list:
            prefix = prefix + '[' + str(item) + ']'
        self.prefix = prefix + ' '

    def reset_prefix(self):
        """Set the init prefix again"""
        self.prefix = str(self.init_prefix)

    def set_arg_only(self, final_arg: str = ''):
        """Set only the second argument"""
        self.prefix = self.init_prefix + '[' + final_arg + ']'

    def info(self, message: str):
        full_message = self.prefix + message
        self.logger.info(full_message)

    def debug(self, message: str):
        full_message = self.prefix + message
        self.logger.debug(full_message)

    def warn(self, message: str):
        full_message = self.prefix + message
        self.logger.warning(full_message)

    def error(self, message: str):
        full_message = self.prefix + message
        self.logger.error(full_message)