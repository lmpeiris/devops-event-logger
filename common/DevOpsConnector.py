import logging
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
        self.event_counter = 0

    def add_event(self, event_id, action, time, case, user, user_ref, local_case, info1: str = '', info2: str = '')\
            -> dict:
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
            # note: none of these id values have a continous function meaning, hence str
            event_dict = {'id': str(event_id), 'action': str(action),
                          'time': str(time), 'case': str(case),
                          'user': str(user), 'local_case': local_case,
                          'info1': info1, 'info2': info2, 'ns': str(self.namespace)}
            self.event_logs.append(event_dict)
            self.event_counter += 1
            return event_dict

    def get_all_events(self, event_get_method_list: list, prod_run: bool = False) -> list[dict]:
        """Umbrella method to retreive all events from a gitlab repo"""
        for method in event_get_method_list:
            getattr(self, method)(prod_run)
        return self.event_logs


