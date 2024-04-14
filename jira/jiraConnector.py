import requests
import json
from requests.auth import HTTPBasicAuth
import datetime
import pandas as pd
import traceback


class JiraConnector:
    def __init__(self, jira_url, auth_token, auth_email):
        self.auth = HTTPBasicAuth(auth_email, auth_token)
        self.headers = {"Accept": "application/json"}
        self.jira_url = jira_url
        print('Jira base url: ' + jira_url)

    def request(self, url_suffix, method: str = "GET", payload: dict = None, params: dict = None) -> dict:
        request_url = self.jira_url + url_suffix
        print('[DEBUG] sending ' + method + ' to : ' + url_suffix)
        try:
            if params is None:
                response = requests.request(method, request_url, headers=self.headers, auth=self.auth)
            else:
                response = requests.request(method, request_url, headers=self.headers, auth=self.auth,
                                            params=params)
            if response.status_code > 399:
                print('[WARN] received error code ' + str(response.status_code) + ' when calling ' + request_url)
                print(response.text)
            else:
                return json.loads(response.text)
        except:
            print('[ERROR] Error occured in jira api request. ignoring')
            traceback.print_exc()
            return {}

    def get_data(self, url_suffix: str, params: dict = None) -> dict:
        response = self.request(url_suffix, 'GET', None, params)
        return response

    def get_email_by_user_id(self, jira_user_id: str) -> str:
        url_suffix = '/rest/api/3/user'
        response = self.get_data(url_suffix, {'accountId': jira_user_id})
        return response['emailAddress']

    def get_change_log(self, issue_key) -> list[dict]:
        url_suffix = '/rest/api/3/issue/' + issue_key + '/changelog'
        response = self.get_data(url_suffix)
        # initialize event_logs return array
        event_logs = []
        try:
            for event in response['values']:
                event_id = str(event['id'])
                event_time = self.strip_tz_get_pd_timestamp(event['created'])
                action = ''
                if 'emailAddress' in event['author']:
                    actor_email = event['author']['emailAddress']
                else:
                    # users without emails are most probably bots
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
                        event_dict = {'id': event_id, 'title': '', 'action': action, 'user': actor_email,
                                      'time': event_time, 'case': issue_key}
                        event_logs.append(event_dict)
        except KeyError as e:
            print('[ERROR] KeyError occured: ' + str(e))
            traceback.print_exc()
        return event_logs

    def get_comments(self, issue_key) -> list[dict]:
        url_suffix = '/rest/api/3/issue/' + issue_key + '/comment'
        response = self.get_data(url_suffix)
        # initialize event_logs return array
        event_logs = []
        try:
            for event in response['comments']:
                event_id = str(event['id'])
                # we do not need bot comments
                if 'emailAddress' in event['author']:
                    actor_email = event['author']['emailAddress']
                    event_time = self.strip_tz_get_pd_timestamp(event['created'])
                    action = 'jira_commented'
                    event_dict = {'id': event_id, 'title': '', 'action': action, 'user': actor_email,
                                  'time': event_time, 'case': issue_key}
                    event_logs.append(event_dict)
        except KeyError as e:
            print('[ERROR] KeyError occured: ' + str(e))
            traceback.print_exc()
        return event_logs

    @classmethod
    def strip_tz_get_pd_timestamp(cls, iso_datetime_with_tz):
        #TODO: check whether this messes up the time if jira reports different tz
        return pd.Timestamp(datetime.datetime.fromisoformat(iso_datetime_with_tz).replace(tzinfo=None))
