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
        # pandas convertible issue list
        self.issue_list = []
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
        """Umbrella method to retrieve all events if event_logs reset is NOT in place"""
        for method in event_get_method_list:
            getattr(self, method)(prod_run)
        return self.event_logs

    def log_status(self, current_count: int, total_count: int = 0):
        cur_progress = str(self.event_counter)
        if total_count == 0:
            completion = str(current_count)
        else:
            completion = str(round(current_count / total_count * 100, 2)) + '%'
        self.logger.info('Events found so far ' + cur_progress + ', items completed: ' + completion)

    def added_event_count(self) -> int:
        """Gives event count added from the last time this was called and resets"""
        added_count = self.temp_event_count
        self.temp_event_count = 0
        return added_count

    @classmethod
    def add_link(cls, target_dict: dict, key, value):
        """lookup dict and add entry to set, else create new set"""
        if key not in target_dict:
            # if set does not exist, we create
            target_dict[key] = {value}
        else:
            # else we add
            target_dict[key].add(value)

    @classmethod
    def empty_set_or_value(cls, check_dict: dict, key) -> set:
        """lookup dict and return set value, or else return empty set"""
        if key not in check_dict:
            return set()
        else:
            return check_dict[key]

    @classmethod
    def get_max_timed_id(cls, input_ids: set, dt_dict_to_lookup: dict) -> int:
        """Gives the entity id which has the max date by reading time from a dict"""
        latest_id = 0
        latest_time = datetime.datetime.fromisoformat('2000-01-01T00:00:00.000Z')
        for i in input_ids:
            itime = datetime.datetime.fromisoformat(dt_dict_to_lookup[i])
            if itime > latest_time:
                latest_time = itime
                latest_id = i
        return latest_id
