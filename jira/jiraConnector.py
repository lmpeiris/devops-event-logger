import requests
import json
from requests.auth import HTTPBasicAuth
import datetime
import pandas as pd
import traceback
import logging
import sys
sys.path.insert(0, '../common')
from DevOpsConnector import DevOpsConnector


class JiraConnector(DevOpsConnector):
    def __init__(self, jira_url, auth_token, namespace, auth_email):
        DevOpsConnector.__init__(self, namespace)
        self.auth = HTTPBasicAuth(auth_email, auth_token)
        self.headers = {"Accept": "application/json"}
        self.jira_url = jira_url
        self.logger.info('Jira base url: ' + jira_url)

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

    def get_email_by_user_id(self, jira_user_id: str) -> str:
        """This method sends an api call to /rest/api/3/user"""
        url_suffix = '/rest/api/3/user'
        response = self.get_data(url_suffix, {'accountId': jira_user_id})
        return response['emailAddress']

    def get_change_log_per_issue(self, issue_key) -> list[dict]:
        """get changelog history via call to /rest/api/3/issue"""
        self.logger.set_prefix([issue_key])
        self.event_logs = []
        url_suffix = '/rest/api/3/issue/' + issue_key + '/changelog'
        response = self.get_data(url_suffix)
        try:
            for event in response['values']:
                event_id = str(event['id'])
                event_time = self.strip_tz_get_pd_timestamp(event['created'])
                action = ''
                if 'emailAddress' in event['author']:
                    actor_email = event['author']['emailAddress']
                else:
                    # assumption: users without emails are most probably bots
                    # TODO: this assumption is not always true. put a proper logic to handle
                    actor_email = event['author']['displayName']
                    action = 'bot_activity'
                for item in event['items']:
                    match item['field']:
                        case 'assignee':
                            action = 'jira_assigned'
                        # case 'resolution':
                        #     action = item['toString']
                        case 'status':
                            action = item['toString']
                        case 'timespent':
                            action = 'time_logged'
                    if action != '':
                        self.add_event(event_id, action, event_time, issue_key, actor_email, '', issue_key)
        except KeyError as e:
            self.logger.error('KeyError occured: ' + str(e))
            traceback.print_exc()
        self.logger.info('number of change log related events found: ' + str(len(self.event_logs)))
        return self.event_logs

    def get_comments_per_issue(self, issue_key) -> list[dict]:
        """Get comment details for a given issue via /rest/api/3/issue/"""
        self.logger.set_prefix([issue_key])
        self.event_logs = []
        url_suffix = '/rest/api/3/issue/' + issue_key + '/comment'
        response = self.get_data(url_suffix)
        try:
            for event in response['comments']:
                event_id = str(event['id'])
                # we do not need bot comments
                # TODO: this assumption is not always true. put a proper logic to handle
                if 'emailAddress' in event['author']:
                    actor_email = event['author']['emailAddress']
                    event_time = self.strip_tz_get_pd_timestamp(event['created'])
                    action = 'jira_commented'
                    self.add_event(event_id, action, event_time, issue_key, actor_email, '', issue_key)
        except KeyError as e:
            print('[ERROR] KeyError occured: ' + str(e))
            traceback.print_exc()
        self.logger.info('number of comments related events found: ' + str(len(self.event_logs)))
        return self.event_logs

    @classmethod
    def strip_tz_get_pd_timestamp(cls, iso_datetime_with_tz):
        """Converts iso datetime with tz to pandas timestamp, without tz - without time conversions"""
        #TODO: check whether this messes up the time if jira reports different tz
        return pd.Timestamp(datetime.datetime.fromisoformat(iso_datetime_with_tz).replace(tzinfo=None))
