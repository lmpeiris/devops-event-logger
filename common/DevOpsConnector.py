import logging
import datetime
import re
import pandas as pd
from LMPLogger import LMPLogger
from LMPUtils import LMPUtils


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
        # iso 8601 regex
        self.iso8601_re = re.compile(r'\d{4}-\d{2}-\d{2}T')

    def add_event(self, event_id, action, iso8601_time, case, user, user_ref, local_case, info1: str = '', info2: str = '',
                  ns: str = '', duration: int = 0) -> dict:
        """Appends an event to event queue, there is no unique validation here.
        If needed use the return as well to get event dict in standard form"""
        fields_ok = True
        # carry out None checks
        for i in event_id, action, iso8601_time, case, user, user_ref, local_case:
            if i is None:
                fields_ok = False
        if fields_ok:
            # TODO: enable data privacy setting to encrypt these info. email as key should be hashed when passed,
            #  this function will only hash user_ref
            # adding to dump later as a user reference from all events
            self.user_ref[str(user)] = str(user_ref)
            if ns == '':
                ns = self.namespace
            # check whether time is in iso8601 format, and is a time and not a date
            time = str(iso8601_time)
            if not self.iso8601_re.search(time):
                self.logger.warn(action + ' event rejected as not a valid iso8601 datetime: ' + time)
                return {}
            # note: none of these id values have a continuous function meaning, hence str
            event_dict = {'id': str(event_id), 'action': str(action),
                          'time': time, 'case': str(case),
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

    def publish_df(self, df: pd.DataFrame, time_columns: list,
                   preserve_timezone: bool, entity_name: str, file_path_name: str):
        """Gives info and saves dataframe as parquet"""
        self.logger.set_prefix(['DF', entity_name])
        self.logger.info('================= ' + entity_name + ' =================')
        for i in time_columns:
            self.logger.debug('Transforming time fields in column: ' + i)
            df[i] = LMPUtils.iso_to_datetime64(df[i], preserve_timezone)
        self.logger.info('Glance of the records: ')
        print(df)
        self.logger.info('Summary: ')
        print(df.info())
        parquet_filename = file_path_name + '.parquet.gz'
        df.to_parquet(parquet_filename, compression='gzip')
        self.logger.info('Pandas Dataframe written to ' + parquet_filename)

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


class ALMConnector(DevOpsConnector):
    def __init__(self, namespace: str, ext_issue_ref_regex: str, api_delay: int, case_type_prefixes: dict):
        DevOpsConnector.__init__(self, namespace, api_delay)
        self.case_type_prefixes = case_type_prefixes
        # this should be overwritten by subclasses
        self.project_id = 0
        self.ext_issue_regex = re.compile(ext_issue_ref_regex)
        # -------------------------------------------
        # -- dicts for fast ref
        # -------------------------------------------
        self.mr_issue_link_dict = {}
        self.mr_issue_mention_dict = {}
        self.issue_created_dict = {}
        self.mr_case_id = {}
        self.mr_created_dict = {}
        self.commit_mr_pre_merge_dict = {}
        self.commit_mr_post_merge_dict = {}
        self.commit_info = {}
        self.commit_mr_commits_dict = {}

    def generate_case_id(self, value, prefix_type: str) -> str:
        """Case id will be generated according to case_type_prefixes"""
        prefix = ''
        if prefix_type in self.case_type_prefixes:
            prefix = self.case_type_prefixes[prefix_type]
        return prefix + '-' + str(self.project_id) + '-' + str(value)

    def find_case_id_for_mr(self, mr_str: str) -> tuple[str, str]:
        """Get the case id for a given MR. Returns case_id, link_type as tuple"""
        linked = self.mr_issue_link_dict
        mentioned = self.mr_issue_mention_dict
        if (mr_str in linked) and len(linked[mr_str]) > 0:
            latest_issue = self.get_max_timed_id(linked[mr_str], self.issue_created_dict)
            case_id = self.generate_case_id(latest_issue, 'issue')
            link_type = 'mr_link'
        elif (mr_str in mentioned) and len(mentioned[mr_str]) == 1:
            issue_iid = next(iter(mentioned[mr_str]))
            self.logger.warn('linking MR to issue using mentions: ' + mr_str)
            case_id = self.generate_case_id(issue_iid, 'issue')
            link_type = 'mr_mention'
        else:
            self.logger.warn('no relation found to an issue for MR : ' + mr_str)
            case_id = self.generate_case_id(mr_str, 'mr')
            link_type = 'undefined'
        return case_id, link_type

    def find_ext_issue_id(self, input_text: str) -> str:
        """Find external system issued ticket id using regex"""
        # we are considering the first match only
        result = self.ext_issue_regex.search(input_text)
        if result is None:
            return ''
        else:
            match = result.group(1)
            self.logger.debug('found reference to external issue id: ' + match)
            return match