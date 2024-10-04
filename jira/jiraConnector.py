import requests
import json
from requests.auth import HTTPBasicAuth
import datetime
import pandas as pd
import pandas.core.frame
import traceback
import time
import sys
sys.path.insert(0, '../common')
from DevOpsConnector import DevOpsConnector


class JiraConnector(DevOpsConnector):
    def __init__(self, jira_url, auth_token, namespace, auth_email, api_delay: int = 1):
        DevOpsConnector.__init__(self, namespace, api_delay)
        self.auth = HTTPBasicAuth(auth_email, auth_token)
        self.headers = {"Accept": "application/json"}
        self.jira_url = jira_url
        self.logger.info('Jira base url: ' + jira_url)
        # this is not user_ref; this gives email for jira id (reverse)
        self.jira_id_email = {}
        # input - issue iid, out - case id
        self.issue_case_id = {}
        # use this for referencing issues internally
        self.issue_df = pd.DataFrame()

    def request(self, url_suffix, method: str = "GET", payload: dict = None, params: dict = None) -> dict:
        """Request sending method with error checking and logging integrated"""
        request_url = self.jira_url + url_suffix
        self.logger.debug('sending ' + method + ' to : ' + url_suffix)
        try:
            if params is None:
                response = requests.request(method, request_url, headers=self.headers, auth=self.auth)
            else:
                response = requests.request(method, request_url, headers=self.headers, auth=self.auth,
                                            params=params)
            if response.status_code > 399:
                self.logger.warn('received error code ' + str(response.status_code) + ' when calling ' + request_url)
                self.logger.warn(response.text)
            else:
                return json.loads(response.text)
        except:
            self.logger.error('Error occured in jira api request. ignoring')
            traceback.print_exc()
            return {}

    def get_data(self, url_suffix: str, params: dict = None) -> dict:
        """Wrapper method for http get calls to jira api"""
        response = self.request(url_suffix, 'GET', None, params)
        return response

    def get_email_by_account_id(self, jira_account_id: str) -> str:
        """This method sends an api call to /rest/api/3/user"""
        # avoid using same api call again
        if jira_account_id in self.jira_id_email:
            user_email = self.jira_id_email[jira_account_id]
        else:
            url_suffix = '/rest/api/3/user'
            self.logger.debug('Getting email for account: ' + str(jira_account_id))
            response = self.get_data(url_suffix, {'accountId': jira_account_id})
            if 'emailAddress' in response:
                user_email = response['emailAddress']
            else:
                self.logger.warn('Email not found for account id: ' + str(jira_account_id))
                user_email = jira_account_id
            self.jira_id_email[jira_account_id] = user_email
        return user_email

    def get_change_log_per_issue(self, issue_key) -> list[dict]:
        """get changelog history via call to /rest/api/3/issue"""
        self.logger.set_prefix([issue_key])
        self.event_counter = 0
        url_suffix = '/rest/api/3/issue/' + issue_key + '/changelog'
        response = self.get_data(url_suffix)
        try:
            for event in response['values']:
                event_id = str(event['id'])
                event_time = self.strip_tz_get_pd_timestamp(event['created'])
                display_name = event['author']['displayName']
                # emailAddress may not always be provided
                if 'emailAddress' in event['author']:
                    user_email = event['author']['emailAddress']
                else:
                    account_id = event['author']['accountId']
                    user_email = self.get_email_by_account_id(account_id)
                action = ''
                for item in event['items']:
                    duration = 0
                    match item['field']:
                        case 'assignee':
                            action = 'jira_assigned'
                        # case 'resolution':
                        #     action = item['toString']
                        case 'status':
                            action = 'jira_' + item['toString']
                        case 'timespent':
                            action = 'jira_time_logged'
                            from_dur = 0
                            if item['from'] is not None:
                                from_dur = int(item['from'])
                            duration = int(int(item['to']) - from_dur)
                    if action != '':
                        ns = self.issue_df.at[issue_key, 'Project key']
                        self.add_event(event_id, action, event_time, self.issue_case_id[issue_key], user_email,
                                       display_name, issue_key, '', '', ns, duration)
        except KeyError as e:
            self.logger.error('KeyError occured: ' + str(e))
            traceback.print_exc()
        self.logger.info('number of change log related events found: ' + str(self.event_counter))
        return self.event_logs

    def get_comments_per_issue(self, issue_key) -> list[dict]:
        """Get comment details for a given issue via /rest/api/3/issue/"""
        self.logger.set_prefix([issue_key])
        self.event_counter = 0
        url_suffix = '/rest/api/3/issue/' + issue_key + '/comment'
        response = self.get_data(url_suffix)
        try:
            for event in response['comments']:
                event_id = str(event['id'])
                # we do not need bot comments
                # TODO: this assumption is not always true. put a proper logic to handle
                # emailAddress may not always be provided
                if 'emailAddress' in event['author']:
                    user_email = event['author']['emailAddress']
                else:
                    account_id = event['author']['accountId']
                    user_email = self.get_email_by_account_id(account_id)
                display_name = event['author']['displayName']
                event_time = self.strip_tz_get_pd_timestamp(event['created'])
                action = 'jira_commented'
                ns = self.issue_df.at[issue_key, 'Project key']
                self.add_event(event_id, action, event_time, self.issue_case_id[issue_key], user_email, display_name,
                               issue_key, '', '', ns)

        except KeyError as e:
            print('[ERROR] KeyError occured: ' + str(e))
            traceback.print_exc()
        self.logger.info('number of comments related events found: ' + str(self.event_counter))
        return self.event_logs

    def iterate_issues(self, issue_df: pandas.core.frame.DataFrame):
        issue_count = 0
        # remove any missing values with 'na' in parent field
        issue_df["Parent"] = issue_df["Parent"].fillna('na')
        # set dataframe as object property for easy referring
        self.issue_df = issue_df
        for jira_issue_key in issue_df.index:
            issue_count = issue_count + 1
            time.sleep(self.api_delay)
            # add entry for create event
            row = issue_df.loc[jira_issue_key]
            # TODO: read from configmap
            account_id = row['Reporter Id']
            display_name = row['Reporter']
            issue_type = row['Issue Type']
            epic_parent = row['Parent']
            issue_id = row['Issue id']
            event_time = row['Created']
            ns = row['Project key']
            user_email = self.get_email_by_account_id(account_id)
            # Note: id field should be kept as string object for compatibility with hashes
            # epic case detection logic
            case_id = jira_issue_key
            action = 'jira_created'
            if epic_parent != 'na':
                case_id = epic_parent
                action = 'jira_sub_created'
            self.issue_case_id[jira_issue_key] = case_id
            self.add_event(str(issue_id), action, event_time, case_id,
                           user_email, display_name, jira_issue_key, issue_type, '', ns)
            # get events from changelog
            self.get_change_log_per_issue(jira_issue_key)
            # get comment events
            self.get_comments_per_issue(jira_issue_key)
            cur_progress = str(len(self.event_logs))
            self.logger.info('Events found so far ' + cur_progress + ', issues completed: ' + str(issue_count))

    @classmethod
    def strip_tz_get_pd_timestamp(cls, iso_datetime_with_tz):
        """Converts iso datetime with tz to pandas timestamp, without tz - without time conversions"""
        #TODO: check whether this messes up the time if jira reports different tz
        return pd.Timestamp(datetime.datetime.fromisoformat(iso_datetime_with_tz).replace(tzinfo=None))
