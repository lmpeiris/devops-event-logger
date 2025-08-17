import logging
import datetime
from LMPLogger import LMPLogger


class DevOpsConnector:
    def __init__(self, namespace: str, api_delay: int):
        logger = logging.getLogger('scriptLogger')
        self.logger = LMPLogger(str(namespace), logger)
        self.namespace = namespace
        self.api_delay = api_delay
        # --- variable initialization ----
        # input - user (gitlab id or email), out - user ref
        self.user_ref = {}
        # event logs will only keep method scope and should be emptied when new method starts
        self.event_logs = []
        # global event count
        self.event_counter = 0
        # temp event count, calling added_event_count method will reset it
        self.temp_event_count = 0

    def add_event(self, event_id, action, time, case, user, user_ref, local_case, info1: str = '', info2: str = '',
                  ns: str = '', duration: int = 0) -> dict:
        """Appends an event to event queue, there is no unique validation here.
        If needed use the return as well to get event dict in standard form"""
        fields_ok = True
        # carry out None checks
        for i in event_id, action, time, case, user, user_ref, local_case:
            if i is None:
                fields_ok = False
        if fields_ok:
            # TODO: enable data privacy setting to encrypt these info. email as key should be hashed when passed,
            #  this function will only hash user_ref
            # adding to dump later as a user reference from all events
            self.user_ref[str(user)] = str(user_ref)
            if ns == '':
                ns = self.namespace
            # note: none of these id values have a continuous function meaning, hence str
            event_dict = {'id': str(event_id), 'action': str(action),
                          'time': str(time), 'case': str(case),
                          'user': str(user), 'local_case': local_case,
                          'info1': info1, 'info2': info2, 'ns': str(ns), 'duration': duration}
            self.event_logs.append(event_dict)
            self.event_counter += 1
            self.temp_event_count += 1
            return event_dict

    def get_all_events(self, event_get_method_list: list, prod_run: bool = False) -> list[dict]:
        """Umbrella method to retrieve all events if event_logs reset is in place"""
        for method in event_get_method_list:
            getattr(self, method)(prod_run)
        return self.event_logs

    def log_status(self, current_count):
        cur_progress = str(len(self.event_logs))
        self.logger.info('Events found so far ' + cur_progress + ', items completed: ' + str(current_count))

    def added_event_count(self) -> int:
        """Gives event count added from the last time this was called and resets"""
        added_count = self.temp_event_count
        self.temp_event_count = 0
        return added_count

    @classmethod
    def rfc2822_to_iso(cls, rfc2822_string: str) -> datetime.datetime:
        """converts Thu, 26 Sep 2024 09:37:22 +0530 like date to 2024-09-26 09:37:22"""
        iso_datetime = datetime.datetime.strptime(rfc2822_string, '%a, %d %b %Y %H:%M:%S %z')
        return iso_datetime
