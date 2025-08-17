import requests
import json
from requests.auth import HTTPBasicAuth
import traceback
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
        # pandas convertible issue list
        self.issue_list = []
        # this is not user_ref; this gives email for jira id (reverse)
        self.jira_id_email = {}
        # input - issue iid, out - case id
        self.issue_case_id = {}
        # input - issue iid, out - ns
        self.issue_ns = {}

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

    def get_change_log_per_issue(self, issue_key: str) -> list[dict]:
        """get changelog history via call to /rest/api/3/issue"""
        self.logger.set_prefix([issue_key])
        self.event_counter = 0
        url_suffix = '/rest/api/3/issue/' + issue_key + '/changelog'
        response = self.get_data(url_suffix)
        try:
            for event in response['values']:
                event_id = str(event['id'])
                event_time = event['created']
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
                        self.add_event(event_id, action, event_time, self.issue_case_id[issue_key], user_email,
                                       display_name, issue_key, '', '', self.issue_ns[issue_key], duration)
        except KeyError as e:
            self.logger.error('KeyError occured: ' + str(e))
            traceback.print_exc()
        self.logger.info('number of change log related events found: ' + str(self.event_counter))
        return self.event_logs

    def get_comments_per_issue(self, issue_key: str) -> list[dict]:
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
                event_time = event['created']
                action = 'jira_commented'
                self.add_event(event_id, action, event_time, self.issue_case_id[issue_key], user_email, display_name,
                               issue_key, '', '', self.issue_ns[issue_key])

        except KeyError as e:
            print('[ERROR] KeyError occured: ' + str(e))
            traceback.print_exc()
        self.logger.info('number of comments related events found: ' + str(self.event_counter))
        return self.event_logs

    def iterate_xml_issues(self, jira_xml: dict, prod_run: bool = False):
        issue_count = 0
        xml_issues = jira_xml['rss']['channel']['item']
        if not prod_run:
            xml_issues = xml_issues[0:20]
        for issue in xml_issues:
            issue_count += 1
            # issue key is the jira project_key - number format string
            issue_key = issue['key']['#text']
            ns = issue['project']['@key']
            case_id = issue_key
            action = 'jira_created'
            issue_id = str(issue['key']['@id'])
            issue_created = self.rfc2822_to_iso(issue['created'])
            issue_type = issue['type']['#text']
            reporter_email = self.get_email_by_account_id(issue['reporter']['@accountid'])
            reporter_name = issue['reporter']['#text']
            if 'parent' in issue:
                parent = issue['parent']['#text']
                case_id = parent
                action = 'jira_sub_created'
            else:
                parent = 'na'
            # set issue dict for easy reference
            self.issue_case_id[issue_key] = case_id
            self.issue_ns[issue_key] = ns
            # add jira create event
            self.add_event(issue_id, action, issue_created, case_id, reporter_email, reporter_name,
                           issue_key, issue_type, '', ns)
            if 'timespent' in issue:
                timespent = int(issue['timespent']['@seconds'])
            else:
                timespent = 0
            # get comment data using api call because xml is not great for lists
            self.added_event_count()
            self.get_comments_per_issue(issue_key)
            comment_count = str(self.added_event_count())
            # get changelog events using api call
            self.get_change_log_per_issue(issue_key)
            # add to issue list
            self.issue_list.append({'issue_key': issue_key, 'reporter_email': reporter_email,
                                    'reporter_name': reporter_name,
                                    'issue_type': issue_type, 'parent': parent,
                                    'issue_id': issue_id, 'created': issue_created,
                                    'ns': ns, 'timespent': timespent,
                                    'comments': comment_count, 'state_changes': str(self.added_event_count())})
            self.log_status(issue_count)



