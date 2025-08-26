from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
from azure.devops.v7_1.work_item_tracking.models import Wiql
from typing import Literal
import re
import traceback
import sys
sys.path.insert(0, '../common')
from DevOpsConnector import DevOpsConnector


class AZDConnector(DevOpsConnector):
    def __init__(self, base_url: str, pvt_token: str, project_name: str, ext_issue_ref_regex: str, api_delay: int = 0):
        DevOpsConnector.__init__(self, project_name, api_delay)
        self.logger.info('Running test auth to: ' + base_url)
        credentials = BasicAuthentication('', pvt_token)
        connection = Connection(base_url=base_url, creds=credentials)
        self.core = connection.clients.get_core_client()
        self.git = connection.clients.get_git_client()
        self.wit = connection.clients.get_work_item_tracking_client()
        self.project = self.core.get_project(project_id=project_name)
        project_id = self.project.id
        self.project_name = project_name
        self.issue_issue_mention_dict = {}
        self.logger.info('==================================================================')
        self.logger.info(' Project id is: ' + str(project_id))

    def get_commit_events(self):
        commits = self.git.get_commits(
            repository_id=self.project_name,
            project=self.project_name,
            search_criteria=None
        )
        commit = commits[0]
        print(commit.as_dict())

    def get_issues_events(self):
        self.logger.info('scanning issues in project_id: ' + str(self.project_name))
        # --initialising values---
        mention_regex = re.compile('mentioned work item #\\d+')
        # WIQL supports SQL like syntax
        # we will be getting work item ids first in to a list
        wiql_query = Wiql(
            query="SELECT [System.Id] FROM WorkItems WHERE [System.TeamProject] = \'" + self.project_name + '\''
        )
        query_result = self.wit.query_by_wiql(wiql_query)
        work_item_ids = [item.id for item in query_result.work_items]
        # And then pull data for all ids at once using single api call
        work_items_batch = self.wit.get_work_items(
            ids=work_item_ids,
            project=self.project_name,
            expand='all' # Use 'all' to get relations, links, etc.
        )
        self.logger.info('number of issues found for project: ' + str(len(work_items_batch)))
        issue_counter = 0
        for item in work_items_batch:
            # work items are considered as issues from now on
            issue_counter += 1
            mentioned_mrs = set()
            mentioned_issues = set()
            item_dict = item.as_dict()
            fields = item_dict['fields']
            issue_id = fields['System.Id']
            self.logger.set_arg_only('AZDI-' + str(issue_id))
            try:
                created_time = fields['System.CreatedDate']
                updated_time = fields['System.ChangedDate']
                author_email = fields['System.CreatedBy']['uniqueName']
                author_name = fields['System.CreatedBy']['displayName']
                issue_type = fields['System.WorkItemType']
                state = fields['System.State']
                for x in item.relations:
                    relation = x.as_dict()
                    match relation['attributes']['name']:
                        case 'Parent':
                            url = relation['url']
                            parent_id = url.split('/')[-1]
                            print(parent_id)
                        case 'Related':
                            if re.search(mention_regex, relation['attributes']['comment']) is not None:
                                # get MR iid and add as int
                                mentioned_issue = int(relation['url'].split('/')[-1])
                                # add to set
                                mentioned_issues.add(mentioned_issue)
                                self.add_link(self.issue_issue_mention_dict, issue_id, mentioned_issue)

                issue_dict = {'id': fields['System.Id'], 'title': fields['System.Description'],
                              'author_id': author_email, 'created_time': created_time, 'type': issue_type,
                              'updated_time': updated_time, 'state': state, 'project_id': self.project_name}
                self.issue_list.append(issue_dict)
                self.log_status(issue_counter, len(work_items_batch))
            except (TypeError, KeyError):
                self.logger.error('Error occurred retrieving data for: ' + str(issue_id) + ' moving to next.')
                traceback.print_exc()
            self.logger.reset_prefix()
        self.logger.info('number of issue related events found: ' + str(self.added_event_count()))