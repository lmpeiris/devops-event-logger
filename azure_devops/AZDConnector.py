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
from ALMConnector import ALMConnector
from LMPUtils import LMPUtils


class AZDConnector(ALMConnector):
    def __init__(self, base_url: str, pvt_token: str, project_name: str, ext_issue_ref_regex: str,
                 case_type_prefixes: dict, api_delay: int = 0):
        ALMConnector.__init__(self, project_name, ext_issue_ref_regex, api_delay, case_type_prefixes)
        self.logger.info('Running test auth to: ' + base_url)
        credentials = BasicAuthentication('', pvt_token)
        connection = Connection(base_url=base_url, creds=credentials)

        # create various connection clients needed for pulling information
        self.core = connection.clients.get_core_client()
        self.git = connection.clients.get_git_client()
        self.wit = connection.clients.get_work_item_tracking_client()
        self.build = connection.clients.get_build_client()
        self.release = connection.clients.get_release_client()

        self.project = self.core.get_project(project_id=project_name)
        project_id = self.project.id
        self.project_name = project_name
        self.issue_issue_mention_dict = {}
        self.logger.info('==================================================================')
        self.logger.info(' Project id is: ' + project_id)
        # this is shortened project id, overides superclass
        self.project_id = project_id[0:7]

    def get_release_completed_time(self, environments: list):
        # this assumes the last job finish time as the completed time of the release
        latest_end_time = None
        for environment in environments:
            for deployment in environment.deploy_steps:
                for release_deploy_phase in deployment.release_deploy_phases:
                    for deployment_job in release_deploy_phase.deployment_jobs:
                        # Check if the stage has a completion timestamp
                        if deployment_job.job.finish_time:
                            # If this is the first one we've found, set it as the latest
                            if latest_end_time is None:
                                # environment.deploy_steps[].release_deploy_phases[].deployment_jobs[].job.finish_time
                                latest_end_time = deployment_job.job.finish_time
                            # If this stage finished after the current latest, update it
                            elif deployment_job.job.finish_time > latest_end_time:
                                latest_end_time = deployment_job.job.finish_time
        return latest_end_time

    def get_release_events(self):
        # Get all release definitions in the project
        # Since pipelines and releases are similar mostly similar logic is being used
        self.logger.info('scanning releases in project_id: ' + str(self.project_name))
        definitions = self.release.get_release_definitions(project=self.project_name)
        pl_counter = 0
        for pipeline in definitions:
            pl_counter += 1
            try:
                pl_dict = pipeline.as_dict()
                pl_id = str(pl_dict['id'])
                local_case = self.generate_case_id(pl_id, 'release')
                pl_created = pl_dict['created_on']
                user_email = pl_dict['created_by']['unique_name']
                user_name = pl_dict['created_by']['display_name']
                self.logger.set_arg_only(local_case)
                # case id is local id since pl definition is not dependent on commit
                self.add_event(pl_id, self.action_prefix + '_REL_created', pl_created, local_case, user_email,
                               user_name, local_case, pl_dict['name'], '', str(self.project_id))
                # can run this without definition id scope to reduce number of api calls
                releases = self.release.get_releases(project=self.project_name, definition_id=pipeline.id)
                for run in releases:
                    b_dict = run.as_dict()
                    # release is unique for a project
                    run_id = str(b_dict['id'])
                    run_created = b_dict['created_on']
                    user_email = b_dict['created_by']['unique_name']
                    user_name = b_dict['created_by']['display_name']
                    release = self.release.get_release(project=self.project_name, release_id=b_dict['id'])
                    # Iterate through all stages (environments) of the release
                    latest_end_time = self.get_release_completed_time(release.environments)
                    if latest_end_time is not None:
                        self.logger.debug('Release completed at: ' + str(latest_end_time))
                        # add event assuming same person completes the release
                        self.add_event(pl_id, self.action_prefix + '_REL_completed', latest_end_time, local_case,
                                       user_email, user_name, local_case, pl_dict['name'], '', str(self.project_id))
                    release_sha = ''
                    release_branch = ''
                    # TODO: get_release also provides stage data including when all stages completed
                    for artifact in release.artifacts:
                        if artifact.type == 'Git' and artifact.is_primary:
                            artifact_dict = artifact.as_dict()
                            release_sha = artifact_dict['definition_reference']['version']['id']
                            release_branch = artifact_dict['definition_reference']['branch']['name']
                    local_case = self.generate_case_id(pl_id, 'release')
                    case_id = self.find_case_id_for_pl(release_sha, run_id, True)
                    # run queued event
                    self.add_event(run_id, self.action_prefix + '_REL_started', run_created, case_id, user_email,
                                   user_name, local_case, pl_dict['name'], b_dict['name'], str(self.project_id))
                    pl_record = {'id': run_id, 'source': release_branch, 'sha': release_sha,
                                 'author': user_email, 'created_time': run_created, 'definition': pl_dict['name'],
                                 'trigger': b_dict['reason'], 'status': b_dict['status'], 'case_id': case_id,
                                 'project_id': self.project_id}
                    self.pl_list.append(pl_record)
                self.log_status(pl_counter, len(definitions))
            except (TypeError, KeyError):
                self.logger.error('Error occurred retrieving data for: ' + str(pipeline.id) + ' moving to next.')
                traceback.print_exc()
            self.logger.reset_prefix()
        self.logger.info('number of pipeline related events found: ' + str(self.added_event_count()))
        return self.event_logs

    def get_pipeline_events(self):
        # Get all pipeline definitions in the project
        self.logger.info('scanning pipelines in project_id: ' + str(self.project_name))
        definitions = self.build.get_definitions(project=self.project_name)
        pl_counter = 0
        for pipeline in definitions:
            pl_counter += 1
            try:
                pl_dict = pipeline.as_dict()
                pl_id = str(pl_dict['id'])
                local_case = self.generate_case_id(pl_id, 'pipeline')
                pl_created = pl_dict['created_date']
                user_email = pl_dict['authored_by']['unique_name']
                user_name = pl_dict['authored_by']['display_name']
                self.logger.set_arg_only(local_case)
                self.logger.debug('reading data for pipeline')
                # case id is local id since pl definition is not dependent on commit
                self.add_event(pl_id, self.action_prefix + '_PL_created', pl_created, local_case, user_email,
                               user_name, local_case, pl_dict['name'], '', str(self.project_id))
                # Get the top 5 most recent runs for this pipeline definition
                builds = self.build.get_builds(project=self.project_name, definitions=[pipeline.id])
                for run in builds:
                    b_dict = run.as_dict()
                    # run id is unique for a project
                    run_id = str(b_dict['id'])
                    run_created = b_dict['start_time']
                    user_email = b_dict['requested_for']['unique_name']
                    user_name = b_dict['requested_for']['display_name']
                    run_sha = b_dict['source_version']
                    local_case = self.generate_case_id(pl_id, 'pipeline')
                    # commit ids map to the build, not the definition
                    case_id = self.find_case_id_for_pl(run_sha, run_id)
                    # run queued event
                    self.add_event(run_id, self.action_prefix + '_PL_started', run_created, case_id, user_email,
                                   user_name, local_case, '', '', str(self.project_id))
                    duration = 0
                    if 'finish_time' in b_dict:
                        # add build completed event
                        run_finished = b_dict['finish_time']
                        self.add_event(run_id, self.action_prefix + '_PL_completed', run_finished, case_id,
                                       user_email, user_name, local_case, '', '', str(self.project_id))
                        duration = LMPUtils.get_seconds_difference_same_zone(run_created, run_finished)
                    pl_record = {'id': run_id, 'source': b_dict['source_branch'], 'sha': run_sha,
                                 'author': user_email, 'created_time': run_created,
                                 'duration': duration, 'status': b_dict['status'], 'case_id': case_id,
                                 'project_id': self.project_id}
                    self.pl_list.append(pl_record)
                self.log_status(pl_counter, len(definitions))
            except (TypeError, KeyError):
                self.logger.error('Error occurred retrieving data for: ' + str(pipeline.id) + ' moving to next.')
                traceback.print_exc()
            self.logger.reset_prefix()
        self.logger.info('number of pipeline related events found: ' + str(self.added_event_count()))
        return self.event_logs

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
                # NOTE: if using as mr.creation_date via library method, it would return a datetime, but we expect str
                #       no issues for pandas in UTC but some functions fail
                self.mr_created_dict[mr_id] = mr_dict['creation_date']
                author_email = mr_dict['created_by']['unique_name']
                author_name = mr_dict['created_by']['display_name']
                # create event log for create MR event
                self.add_event(mr_id, self.action_prefix + '_MR_created', mr_dict['creation_date'], case_id,
                               author_email, author_name, local_case, '', '', str(self.project_id))
                # get data from PR event thread
                # this reads comments as well as review updates
                pr_review_regex = re.compile(r'([+-]?\d+)$')
                pr_complete_regex = re.compile('^.*updated the pull request status to Completed$')
                pr_abandoned_regex = re.compile('^.*updated the pull request status to Abandoned$')
                vote_map = {
                    10: "_MR_approved",
                    5: "_MR_appr_sug",
                    -5: "_MR_wait_author",
                    -10: "_MR_rejected"
                }
                mr_threads = self.git.get_threads(repository_id=repo_id, pull_request_id=mr_id,
                                                  project=self.project_name)
                for thread in mr_threads:
                    thread_dict = thread.as_dict()
                    for comment in thread_dict['comments']:
                        comment_created = comment['published_date']
                        comment_author = comment['author']['unique_name']
                        comment_name = comment['author']['display_name']
                        # log unknown actions for now
                        action = self.action_prefix + '_MR_comment_UNKNOWN'
                        if comment['comment_type'] == 'system':
                            review_match = pr_review_regex.search(comment['content'])
                            comp_match = pr_complete_regex.match(comment['content'])
                            aban_match = pr_abandoned_regex.match(comment['content'])
                            if review_match is not None:
                                review_score = int(review_match.group(1))
                                # need to use vote map to identify what has happened, no other way
                                action = self.action_prefix + vote_map[review_score]
                            elif comp_match is not None:
                                action = self.action_prefix + '_MR_completed'
                            elif aban_match is not None:
                                action = self.action_prefix + '_MR_abandoned'
                        elif comment['comment_type'] == 'text':
                            action = self.action_prefix + '_MR_commented'
                            self.logger.warn('Unknown comment type: ' + json.dumps(comment))
                        self.add_event(mr_id, action, comment_created, case_id, comment_author,
                                       comment_name, local_case, '', '', str(self.project_id))

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
                linked = set()
                mentioned = set()
                if mr_id in self.mr_issue_link_dict:
                    linked = self.mr_issue_link_dict[mr_id]
                if mr_id in self.mr_issue_mention_dict:
                    mentioned = self.mr_issue_mention_dict[mr_id]
                # add entry to MR dict
                mr_dict = {'id': mr_id, 'title': mr.title, 'author_id': author_email,
                           'created_time': mr_dict['creation_date'], 'state': mr.status, 'source_branch': mr.source_ref_name,
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
                # add event for issue creation
                self.add_event(case_id, self.action_prefix+ '_issue_created', created_time, case_id, author_email,
                               author_name, case_id, '', '', str(self.project_id))
                # get all revisions for the issue
                revisions = self.wit.get_revisions(id=int(issue_id), project=self.project_name, expand='fields')
                # sort revisions since we need to track state changes
                revisions.sort(key=lambda r: r.rev)
                # Loop through revisions to find the differences in the 'System.State' field
                # TODO: see whether this captures assigning to user
                for i in range(1, len(revisions)):
                    current_rev = revisions[i]
                    previous_rev = revisions[i - 1]
                    # Direct comparison of the 'System.State' field
                    current_state = current_rev.fields.get('System.State')
                    previous_state = previous_rev.fields.get('System.State')
                    if current_state != previous_state:
                        self.add_event(case_id + '-' + str(current_rev.rev),
                                       self.action_prefix + '_issue_' + current_state,
                                       current_rev.fields['System.ChangedDate'], case_id,
                                       current_rev.fields['System.ChangedBy']['uniqueName'],
                                       current_rev.fields['System.ChangedBy']['displayName'],
                                       case_id, '', '', str(self.project_id))
                # check relations to other entities
                for x in item.relations:
                    relation = x.as_dict()
                    match relation['attributes']['name']:
                        case 'Parent':
                            url = relation['url']
                            parent_id = url.split('/')[-1]
                            # TODO: is this sub task?
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