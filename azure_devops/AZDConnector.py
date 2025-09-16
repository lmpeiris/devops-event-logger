from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
from azure.devops.v7_1.work_item_tracking.models import Wiql
from azure.devops.v7_1.git.models import GitPullRequestSearchCriteria
from typing import Literal
import re
import traceback
import sys
import json
sys.path.insert(0, '../common')
from DevOpsConnector import ALMConnector


class AZDConnector(ALMConnector):
    def __init__(self, base_url: str, pvt_token: str, project_name: str, ext_issue_ref_regex: str,
                 case_type_prefixes: dict, api_delay: int = 0):
        ALMConnector.__init__(self, project_name, ext_issue_ref_regex, api_delay, case_type_prefixes)
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
        self.logger.info(' Project id is: ' + project_id)
        # this is shortened project id, overides superclass
        self.project_id = project_id[0:7]

    def get_commit_events(self):
        commits = self.git.get_commits(
            repository_id=self.project_name,
            project=self.project_name,
            search_criteria=None
        )
        commit = commits[0]
        print(commit.as_dict())

    def get_mrs_events(self):
        """Extract MR events from repo and analyse relations to issues"""
        self.logger.info('scanning MRs in project_id: ' + str(self.project_name))
        # TODO: link status should be available in issue itself when linked via UI
        # Create a search criteria object to get ALL pull requests (active, completed, abandoned)
        search_criteria = GitPullRequestSearchCriteria(status='all')
        # Get the list of pull requests for the entire project
        merge_requests = self.git.get_pull_requests_by_project(project=self.project_name,
                                                               search_criteria=search_criteria)
        self.logger.info('number of MRs found for project: ' + str(len(merge_requests)))
        mr_counter = 0
        for mr in merge_requests:
            mr_counter += 1
            mr_dict = mr.as_dict()
            mr_id = str(mr_dict['pull_request_id'])
            repo_id = mr_dict['repository']['id']
            # MR id is globally unique within organization
            self.logger.debug('Checking MR: ' + mr_id + ' on repo: ' + repo_id)
            try:
                local_case = self.generate_case_id(mr_id, 'mr')
                self.logger.set_arg_only(local_case)
                self.logger.debug('reading data for MR')
                case_id, link_type = self.find_case_id_for_mr(mr_id)
                self.mr_case_id[mr_id] = case_id
                self.mr_created_dict[mr_id] = mr.creation_date
                author_email = mr_dict['created_by']['unique_name']
                author_name = mr_dict['created_by']['display_name']
                # create event log for create MR event
                self.add_event(mr_id, 'azd_MR_created', mr.creation_date, case_id, author_email, author_name,
                               local_case, '', '', str(self.project_id))
                # get data from PR event thread
                # this reads comments as well as review updates
                pr_review_regex = re.compile(r'([+-]?\d+)$')
                vote_map = {
                    10: "azd_MR_approved",
                    5: "azd_MR_appr_sug",
                    -5: "azd_MR_wait_author",
                    -10: "azd_MR_rejected"
                }
                mr_threads = self.git.get_threads(repository_id=repo_id, pull_request_id=mr_id,
                                                  project=self.project_name)
                for thread in mr_threads:
                    thread_dict = thread.as_dict()
                    for comment in thread_dict['comments']:
                        comment_created = comment['published_date']
                        comment_author = comment['author']['unique_name']
                        comment_name = comment['author']['display_name']
                        if 'CodeReviewThreadType' in thread_dict['properties']:
                            review_score = int(pr_review_regex.search(comment['content']).group(1))
                            # need to use vote map to identify what has happened, no other way
                            action = vote_map[review_score]
                        elif comment['comment_type'] == 'text':
                            action = 'azd_MR_commented'
                        else:
                            # log unknown actions for now
                            action = 'azd_MR_comment_UNKNOWN'
                            self.logger.warn('Unknown comment type: ' + json.dumps(comment))
                        self.add_event(mr_id, action, comment_created, case_id, comment_author,
                                       comment_name, local_case, '', '', str(self.project_id))

                if 'closed_date' in mr_dict:
                    closer_email = author_email
                    closer_name = author_name
                    # TODO this needs some sample data generation with help of other people
                    # TODO also add abondon code
                    if 'closed_by' in mr_dict:
                        closer_email = mr_dict['closed_by']['unique_name']
                        closer_name = mr_dict['closed_by']['display_name']
                    if mr.status == 'completed':
                        # adding merged event
                        self.add_event(mr_id, 'azd_MR_merged', mr.merged_at,
                                       case_id, closer_email, closer_name, local_case, '', '',
                                       str(self.project_id))
                pre_merge_commit = mr_dict['last_merge_source_commit']['commit_id']
                post_merge_commit = mr_dict['last_merge_commit']['commit_id']
                # adding pre-merge commit
                self.add_link(self.commit_mr_pre_merge_dict, pre_merge_commit, mr_id)
                # post-merge pipelines can run from merged commit
                self.add_link(self.commit_mr_post_merge_dict, post_merge_commit, mr_id)
                # find commit events
                # TODO: commits which are not allocated to a MR or squashed will not be found
                commits = self.git.get_pull_request_commits(repository_id=repo_id, pull_request_id=mr_id,
                                                            project=self.project_name)
                self.logger.debug('commits found related to MR: ' + str(len(commits)))
                for commit in commits:
                    commit_dict = commit.as_dict()
                    commit_id = commit_dict['commit_id']
                    commit_email = commit_dict['author']['email']
                    commit_name = commit_dict['author']['name']
                    commit_created = commit_dict['author']['date']
                    # TODO: decide which info to be added here for commits
                    info1 = ''
                    if commit_id == pre_merge_commit: info1 = 'pre_merge_commit'
                    if commit_id == post_merge_commit: info1 = 'post_merge_commit'
                    # we cannot add this event yet since we need to sort out duplicates
                    # as well as links to MRs
                    self.commit_info[commit_id] = {'time': commit_created, 'user': commit_email,
                                                   'user_ref': commit_name,  'info1': info1}
                    # merge pipelines could be launched from any commit related to MR
                    self.add_link(self.commit_mr_commits_dict, commit_id, mr_id)
                # Try to find an external issue id. description can be null
                if 'description' in mr_dict:
                    ext_issue_id = self.find_ext_issue_id(mr_dict['title'] + ' ' + mr_dict['description'])
                else:
                    ext_issue_id = self.find_ext_issue_id(mr_dict['title'])
                # TODO - unedited code from here
                linked = set()
                mentioned = set()
                if mr_id in self.mr_issue_link_dict:
                    linked = self.mr_issue_link_dict[mr_id]
                if mr_id in self.mr_issue_mention_dict:
                    mentioned = self.mr_issue_mention_dict[mr_id]
                # add entry to MR dict
                mr_dict = {'id': mr_id, 'title': mr.title, 'author_id': author_email,
                           'created_time': mr.creation_date, 'state': mr.status, 'source_branch': mr.source_ref_name,
                           'target_branch': mr.target_ref_name, 'project_id': self.project_name,
                           'ext_issue_id': ext_issue_id, 'linked_issues': linked, 'mentioned_issues': mentioned,
                           'case_id': case_id, 'link_type': link_type}
                self.mr_list.append(mr_dict)
                self.log_status(mr_counter, len(merge_requests))
            except (TypeError, KeyError):
                self.logger.error('Error occurred retrieving data for: ' + mr_id + ' moving to next.')
                traceback.print_exc()
            self.logger.reset_prefix()
        self.logger.info('number of MR related events found: ' + str(self.added_event_count()))

    def get_issues_events(self):
        self.logger.info('scanning issues in project_id: ' + str(self.project_name))
        # --initialising values---
        mention_regex = re.compile('mentioned work item #\\d+')
        # WIQL supports SQL like syntax
        # we will be getting work item ids first in to a list
        wiql_query = Wiql(query="SELECT [System.Id] FROM WorkItems WHERE [System.TeamProject] = \'"
                                + self.project_name + '\'')
        query_result = self.wit.query_by_wiql(wiql_query)
        work_item_ids = [item.id for item in query_result.work_items]
        if len(work_item_ids) == 0:
            self.logger.info('No work items found for project. skipping')
            return
        # And then pull data for all ids at once using single api call
        work_items_batch = self.wit.get_work_items(ids=work_item_ids, project=self.project_name, expand='all')
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
            case_id = self.generate_case_id(issue_id, 'issue')
            self.logger.set_arg_only(case_id)
            try:
                created_time = fields['System.CreatedDate']
                updated_time = fields['System.ChangedDate']
                author_email = fields['System.CreatedBy']['uniqueName']
                author_name = fields['System.CreatedBy']['displayName']
                issue_type = fields['System.WorkItemType']
                state = fields['System.State']
                self.add_event(case_id, 'azd_issue_created', created_time, case_id, author_email, author_name,
                               case_id, '', '', str(self.project_id))
                for x in item.relations:
                    relation = x.as_dict()
                    match relation['attributes']['name']:
                        case 'Parent':
                            url = relation['url']
                            parent_id = url.split('/')[-1]
                            print(parent_id)
                        case 'Related':
                            if re.search(mention_regex, relation['attributes']['comment']) is not None:
                                # TODO: add PR mention criteria
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