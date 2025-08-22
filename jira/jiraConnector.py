import requests
import json
import re
from requests.auth import HTTPBasicAuth
import traceback
import sys
sys.path.insert(0, '../common')
from DevOpsConnector import DevOpsConnector
from LMPUtils import LMPUtils


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
        # input - issue iid, out - ns
        self.issue_ns = {}
        # input - issue iid, out - set of issue mentions
        self.issue_mentions = {}
        # regex for finding other jira issue mentions
        self.jira_issue_regex = re.compile(jira_url + '/browse/[A-Z]{1,9}-\\d+')

    def find_issue_id_mentions(self, input_dict: dict, url_split_number: int = 4) -> list[str]:
        """Gives list of issue ids found in dict by converting whole thing to json"""
        json_txt = json.dumps(input_dict)
        regex_matches = []
        for match in self.jira_issue_regex.findall(json_txt):
            regex_matches.append(match.split('/')[url_split_number])
        return regex_matches

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
                return {}
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

    def get_change_log_per_issue(self, issue_key: str) -> dict:
        """get changelog history via call to /rest/api/3/issue"""
        self.logger.set_prefix([issue_key])
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
                        case 'Key':
                            action = "jira_prj_changed"
                    if action != '':
                        self.add_event(event_id, action, event_time, self.issue_case_id[issue_key], user_email,
                                       display_name, issue_key, '', '', self.issue_ns[issue_key], duration)
        except KeyError as e:
            self.logger.error('KeyError occured: ' + str(e))
            traceback.print_exc()
        return response

    def get_comments_per_issue(self, issue_key: str) -> dict:
        """Get comment details for a given issue via /rest/api/3/issue/"""
        self.logger.set_prefix([issue_key])
        url_suffix = '/rest/api/3/issue/' + issue_key + '/comment'
        response = self.get_data(url_suffix)
        # iterate through comments
        self.iterate_comments(response['comments'], issue_key)
        return response

    def get_issue_via_api(self, issue_key: str) -> dict:
        """Get issue details for a given issue via /rest/api/3/issue/{issueIdOrKey}"""
        self.logger.set_prefix([issue_key])
        url_suffix = '/rest/api/3/issue/' + issue_key
        issue = self.get_data(url_suffix)
        # skip if response is empty (aka problem with issue)
        if issue == {}:
            self.logger.error('Issue data cannot be retrieved: ' + issue_key)
            return issue
        try:
            # issue key is the jira project_key - number format string
            ns = issue['fields']['project']['key']
            case_id = issue_key
            action = 'jira_created'
            issue_id = str(issue['id'])
            issue_created = issue['fields']['created']
            issue_type = issue['fields']['issuetype']['name']
            reporter_email = issue['fields']['creator']['emailAddress']
            reporter_name = issue['fields']['creator']['displayName']
            parent = ''
            if 'parent' in issue['fields']:
                parent = issue['fields']['parent']['key']
                case_id = parent
                action = 'jira_sub_created'
            # set issue dict for easy reference
            self.issue_case_id[issue_key] = case_id
            self.issue_ns[issue_key] = ns
            timespent = 0
            if issue['fields']['timetracking'] is not None:
                if 'timeSpentSeconds' in issue['fields']['timetracking']:
                    timespent = int(issue['fields']['timetracking']['timeSpentSeconds'])
            # find any issue mentions
            mentions = self.find_issue_id_mentions(issue['fields']['description'])
            for mention in mentions:
                self.add_link(self.issue_mentions, issue_key, mention)
            # add jira create event
            self.add_event(issue_id, action, issue_created, case_id, reporter_email, reporter_name,
                           issue_key, issue_type, parent, ns)
            # iterate through comments and add, no need to use comments api call for this
            self.added_event_count()
            self.iterate_comments(issue['fields']['comment']['comments'], issue_key)
            comment_count = str(self.added_event_count())
            self.logger.debug('Comment events added: ' + comment_count)
            # get changelog events using api call
            self.get_change_log_per_issue(issue_key)
            changelog_count = str(self.added_event_count())
            self.logger.debug('Changelog events added: ' + changelog_count)
            # prepare mentions as a set
            mention_set = self.empty_set_or_value(self.issue_mentions, issue_key)
            # add to issue list
            self.issue_list.append({'issue_key': issue_key, 'reporter_email': reporter_email,
                                    'reporter_name': reporter_name,
                                    'issue_type': issue_type, 'parent': parent,
                                    'issue_id': issue_id, 'created': issue_created,
                                    'ns': ns, 'timespent': timespent,
                                    'comments': comment_count, 'state_changes': changelog_count,
                                    'mentions': mention_set})
        except KeyError as e:
            print('[ERROR] KeyError occured: ' + str(e))
            traceback.print_exc()
        return issue

    def iterate_comments(self, comment_list: list[dict], issue_key: str):
        try:
            for event in comment_list:
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
                info1 = ''
                if 'parentId' in event:
                    action = 'jira_cm_reply'
                    info1 = event['parentId']
                else:
                    action = 'jira_commented'
                # find any issue mentions
                mentions = self.find_issue_id_mentions(event['body'])
                for mention in mentions:
                    self.add_link(self.issue_mentions, issue_key, mention)
                self.add_event(event_id, action, event_time, self.issue_case_id[issue_key], user_email,
                               display_name,
                               issue_key, info1, '', self.issue_ns[issue_key])
        except KeyError as e:
            print('[ERROR] KeyError occured: ' + str(e))
            traceback.print_exc()

    def iterate_xml_issues(self, jira_xml: dict, prod_run: bool = False):
        """Allows to load issues from a xml dump from jira"""
        issue_counter = 0
        xml_issues = jira_xml['rss']['channel']['item']
        if not prod_run:
            xml_issues = xml_issues[0:10]
        for issue in xml_issues:
            issue_counter += 1
            # issue key is the jira project_key - number format string
            issue_key = issue['key']['#text']
            ns = issue['project']['@key']
            case_id = issue_key
            action = 'jira_created'
            issue_id = str(issue['key']['@id'])
            issue_created = LMPUtils.rfc2822_to_iso(issue['created'])
            issue_type = issue['type']['#text']
            reporter_email = self.get_email_by_account_id(issue['reporter']['@accountid'])
            reporter_name = issue['reporter']['#text']
            parent = ''
            if 'parent' in issue:
                parent = issue['parent']['#text']
                case_id = parent
                action = 'jira_sub_created'
            # set issue dict for easy reference
            self.issue_case_id[issue_key] = case_id
            self.issue_ns[issue_key] = ns
            # add jira create event
            self.add_event(issue_id, action, issue_created, case_id, reporter_email, reporter_name,
                           issue_key, issue_type, parent, ns)
            if 'timespent' in issue:
                timespent = int(issue['timespent']['@seconds'])
            else:
                timespent = 0
            # find any issue mentions
            mentions = self.find_issue_id_mentions(issue['description'])
            for mention in mentions:
                self.add_link(self.issue_mentions, issue_key, mention)
            # get comment data using api call because xml is not great for lists
            self.added_event_count()
            self.get_comments_per_issue(issue_key)
            comment_count = str(self.added_event_count())
            self.logger.debug('Comment events added: ' + comment_count)
            # get changelog events using api call
            self.get_change_log_per_issue(issue_key)
            changelog_count = str(self.added_event_count())
            self.logger.debug('Changelog events added: ' + changelog_count)
            # prepare mentions as a set
            mention_set = self.empty_set_or_value(self.issue_mentions, issue_key)
            # add to issue list
            self.issue_list.append({'issue_key': issue_key, 'reporter_email': reporter_email,
                                    'reporter_name': reporter_name,
                                    'issue_type': issue_type, 'parent': parent,
                                    'issue_id': issue_id, 'created': issue_created,
                                    'ns': ns, 'timespent': timespent,
                                    'comments': comment_count, 'state_changes': changelog_count,
                                    'mentions': mention_set})
            self.log_status(issue_counter, len(xml_issues))



