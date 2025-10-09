# from gitlab.v4.objects import ProjectIssue
import gitlab
import re
import traceback
import sys
sys.path.insert(0, '../common')
from ALMConnector import ALMConnector


class GitlabConnector(ALMConnector):
    def __init__(self, base_url: str, pvt_token: str, project_id: str, ext_issue_ref_regex: str,
                 case_type_prefixes: dict, api_delay: int = 0):
        ALMConnector.__init__(self, project_id, ext_issue_ref_regex,  api_delay, case_type_prefixes)
        self.logger.info('Running test auth to: ' + base_url)
        self.gl = gitlab.Gitlab(base_url, private_token=pvt_token)
        self.gl.auth()
        self.project_id = int(project_id)
        self.project_object = self.gl.projects.get(self.project_id)
        self.logger.info('==================================================================')
        self.logger.info('Project id is: ' + str(project_id) + ' path is: ' + self.project_object.path_with_namespace)
        # -------------------------------------------
        # -- dicts for fast ref
        # -------------------------------------------
        # Please note that some attributes would also come from parent class as well
        # input - issue iid, provides various info
        self.issue_iid_dict = {}
        # input issue iid, out - mr iid(s) linked
        self.issue_mr_link_dict = {}
        # input issue iid, out - mr iid(s) mentioned
        self.issue_mr_mention_dict = {}
        # input - user (gitlab id or email), out - user ref
        self.user_ref = {}
        # input - user email, out - gitlab user id
        self.user_email_map = {}
        # input - branch name, out - case id
        self.branch_case_id = {}

    def get_pipeline_events(self, prod_run: bool = False) -> list[dict]:
        """Extract pipeline and job events from the repo"""
        project = self.project_object
        pipelines = project.pipelines.list(get_all=prod_run)
        self.logger.info('number of pipelines found for project: ' + str(len(pipelines)))
        pl_counter = 0
        for pipeline in pipelines:
            pl_counter += 1
            try:
                # For pipelines, standard is to use global id
                self.logger.set_arg_only('GLPL-' + str(pipeline.id))
                self.logger.debug('reading data for pipeline')
                # Pulling pl again due to https://python-gitlab.readthedocs.io/en/v4.4.0/faq.html#attribute-error-list
                pl = project.pipelines.get(pipeline.id)
                case_id = self.find_case_id_for_pl(pl.sha, pl.id)
                local_case = self.generate_case_id(pl.id, 'pipeline')
                # TODO: create the PL list
                # pipeline created event
                self.add_event(pl.id, self.action_prefix + '_PL_created',  pl.created_at, case_id, pl.user['id'],
                               pl.user['name'], local_case, '', '', str(self.project_id))
                # pipeline completed event - only adds if finished_at is not none
                self.add_event(pl.id, self.action_prefix + '_PL_completed', pl.finished_at, case_id, pl.user['id'],
                               pl.user['name'], local_case, '', '', str(self.project_id))
                # get pipeline jobs
                jobs = pl.jobs.list(get_all=prod_run)
                self.logger.debug('jobs found for pipeline: ' + str(len(jobs)))
                # TODO: better strategy would be to find when the first job of each stage started,
                #  and have one event per stage
                for job in jobs:
                    if job.status in ['started', 'failed', 'success']:
                        # add job event
                        # job started at time could be None - as created jobs may not have run
                        self.add_event(job.id, self.action_prefix + '_job_started', job.started_at, case_id, job.user['id'],
                                       job.user['name'], local_case, str(job.name), str(job.stage), str(self.project_id))
                # create pipeline dict
                if pl.duration is None:
                    duration = 0
                else:
                    duration = pl.duration
                pl_dict = {'id': pl.id, 'source': pl.source, 'sha': pl.sha, 'before_sha': str(pl.before_sha),
                           'author': pl.user['id'], 'created_time': pl.created_at, 'updated_time': pl.updated_at,
                           'duration': duration, 'status': pl.status, 'case_id': case_id, 'project_id': self.project_id}
                self.pl_list.append(pl_dict)
                self.log_status(pl_counter, len(pipelines))
            except (TypeError, KeyError):
                self.logger.error('Error occurred retrieving data for: ' + str(pipeline.id) + ' moving to next.')
                traceback.print_exc()
            self.logger.reset_prefix()
        self.logger.info('number of pipeline related events found: ' + str(self.added_event_count()))
        return self.event_logs

    def get_branch_events(self, prod_run: bool = False) -> list[dict]:
        """Extract branch creation events from repo"""
        project = self.project_object
        self.logger.info('scanning branches in project_id: ' + str(self.project_id))
        branches = project.branches.list(get_all=prod_run)
        for br in branches:
            try:
                if br.name in self.branch_case_id:
                    case_id = self.branch_case_id[br.name]
                else:
                    case_id = self.generate_case_id(br.commit['short_id'], 'branch')
                # NOTE: commit do not provide gitlab user id, but provides email
                author_ref = br.commit['author_email']
                # if we already know gitlab id, then use it instead
                if author_ref in self.user_email_map:
                    author_ref = self.user_email_map[br.commit['author_email']]
                # add branch create event
                # note: br commit id is branch hash
                self.add_event(br.commit['id'], self.action_prefix + '_branch_created', br.commit['created_at'], case_id,
                               author_ref, br.commit['author_name'], case_id, br.name, '', str(self.project_id))
            except (TypeError, KeyError):
                self.logger.error('Error occurred retrieving data for: ' + br.name + ' moving to next.')
                traceback.print_exc()
        self.logger.info('number of branch events found: ' + str(self.added_event_count()))
        return self.event_logs

    def get_mrs_events(self, prod_run: bool = False) -> list[dict]:
        """Extract MR events from repo and analyse relations to issues"""
        self.logger.info('scanning MRs in project_id: ' + str(self.project_id))
        merge_commit_regex = re.compile('Merge branch')
        project = self.project_object
        merge_requests = project.mergerequests.list(get_all=prod_run)
        self.logger.info('number of MRs found for project: ' + str(len(merge_requests)))
        mr_counter = 0
        for mr in merge_requests:
            mr_counter += 1
            try:
                self.logger.set_arg_only('MR-' + str(mr.iid))
                self.logger.debug('reading data for MR')
                case_id, link_type = self.find_case_id_for_mr(mr.iid)
                local_case = self.generate_case_id(mr.iid, 'mr')
                self.mr_case_id[mr.iid] = case_id
                self.mr_created_dict[mr.iid] = mr.created_at
                # create event log for create MR event
                self.add_event(mr.id, self.action_prefix + '_MR_created', mr.created_at, case_id, mr.author['id'],
                               mr.author['name'], local_case, '', '', str(self.project_id))
                if mr.merge_user is not None:
                    # adding merged event
                    # it was observed that sometimes merge user id not defined even if merged_at is defined
                    self.add_event(mr.id, self.action_prefix + '_MR_merged', mr.merged_at,
                                   case_id, mr.merge_user['id'], mr.merge_user['name'], local_case, '', '',
                                   str(self.project_id))
                # add closing event
                if mr.closed_at is not None:
                    self.add_event(mr.id, self.action_prefix + '_MR_closed', mr.closed_at,
                                   case_id, mr.closed_by['id'], mr.closed_by['name'], local_case, '', '',
                                   str(self.project_id))
                # adding pre-merge commit
                self.add_link(self.commit_mr_pre_merge_dict, mr.sha, mr.iid)
                # post-merge pipelines can run from merged commit
                self.add_link(self.commit_mr_post_merge_dict, mr.merge_commit_sha, mr.iid)
                # find commit events
                # TODO: commits which are not allocated to a MR or squashed will not be found
                commits = mr.commits()
                self.logger.debug('commits found related to MR: ' + str(len(commits)))
                for commit in commits:
                    # NOTE: commit do not provide gitlab user id, but provides email
                    author_ref = commit.author_email
                    # if we already know gitlab id, then use it instead
                    if commit.author_email in self.user_email_map:
                        author_ref = self.user_email_map[commit.author_email]
                    # see whether commit is a merge commit
                    info1 = ''
                    if re.search(merge_commit_regex, commit.message) is not None:
                        info1 = 'merge_commit'
                    # we cannot add this event yet since we need to sort out duplicates
                    # as well as links to MRs
                    self.commit_info[commit.id] = {'time': commit.created_at, 'user': author_ref,
                                                   'user_ref': commit.author_name,  'info1': info1}
                    # merge pipelines could be launched from any commit related to MR
                    self.add_link(self.commit_mr_commits_dict, commit.id, mr.iid)
                # Try to find an external issue id. description can be null
                if mr.description is not None:
                    ext_issue_id = self.find_ext_issue_id(mr.title + ' ' + mr.description)
                else:
                    ext_issue_id = self.find_ext_issue_id(mr.title)
                linked = set()
                mentioned = set()
                if mr.iid in self.mr_issue_link_dict:
                    linked = self.mr_issue_link_dict[mr.iid]
                if mr.iid in self.mr_issue_mention_dict:
                    mentioned = self.mr_issue_mention_dict[mr.iid]
                # add entry to MR dict
                mr_dict = {'id': mr.id, 'iid': mr.iid, 'title': mr.title, 'author_id': mr.author['id'],
                           'created_time': mr.created_at,
                           'updated_time': mr.updated_at, 'state': mr.state, 'source_branch': mr.source_branch,
                           'target_branch': mr.target_branch, 'project_id': mr.project_id, 'ext_issue_id': ext_issue_id,
                           'linked_issues': linked, 'mentioned_issues': mentioned, 'case_id': case_id,
                           'link_type': link_type}
                self.mr_list.append(mr_dict)
                self.log_status(mr_counter, len(merge_requests))
            except (TypeError, KeyError):
                self.logger.error('Error occurred retrieving data for: ' + str(mr.iid) + ' moving to next.')
                traceback.print_exc()
            self.logger.reset_prefix()
        self.logger.info('number of MR related events found: ' + str(self.added_event_count()))
        return self.event_logs

    def get_issues_events(self, prod_run: bool = False) -> list[dict]:
        """Get gitlab issue related events, find relations to MRs and external issues"""
        self.logger.info('scanning issues in project_id: ' + str(self.project_id))
        # --initialising values---
        branch_create_regex = re.compile('created branch')
        assigned_regex = re.compile('assigned to')
        mr_regex = re.compile('mentioned in merge request')
        # ----------
        project = self.project_object
        issues = project.issues.list(get_all=prod_run)
        self.logger.info('number of issues found for project: ' + str(len(issues)))
        issue_counter = 0
        for issue in issues:
            try:
                issue_counter += 1
                self.logger.set_arg_only('GLI-' + str(issue.iid))
                linked_mrs = set()
                mentioned_mrs = set()
                self.logger.debug('reading data for issue')
                # update internal reference dict
                self.issue_iid_dict[issue.iid] = {}
                self.issue_iid_dict[issue.iid]['id'] = issue.id
                self.issue_iid_dict[issue.iid]['branches'] = []
                self.issue_created_dict[issue.iid] = issue.created_at
                case_id = self.generate_case_id(issue.iid, 'issue')
                # read through notes to find assign events and branch creation
                notes = issue.notes.list(get_all=prod_run)
                self.logger.debug('notes found for issue: ' + str(len(notes)))
                for note in notes:
                    # TODO: issue comments are not supported yet
                    # check whether there's assigned note
                    if re.search(assigned_regex, note.body) is not None:
                        # add assigned event
                        self.add_event(note.id, self.action_prefix + '_issue_assigned', note.created_at, case_id,
                                       note.author['id'], note.author['name'], case_id, note.body, '',
                                       str(self.project_id))
                    # check whether there's branch creation
                    if re.search(branch_create_regex, note.body) is not None:
                        # get branch name as string
                        issue_branch = note.body.split('`')[1]
                        self.issue_iid_dict[issue.iid]['branches'].append(issue_branch)
                        # Don't add branch create event here, just add branch issue reference
                        # this is because we are pulling the entire list anyway
                        self.branch_case_id[issue_branch] = case_id
                    # identify any merge requests linked
                    if re.search(mr_regex, note.body) is not None:
                        # get MR iid and add as int
                        mentioned_mr = int(note.body.split('!')[1])
                        # add to set
                        mentioned_mrs.add(mentioned_mr)
                        self.add_link(self.mr_issue_mention_dict, mentioned_mr, issue.iid)
                # find issues directly related
                mr_links = issue.closed_by()
                for mr_link in mr_links:
                    link_iid = mr_link['iid']
                    linked_mrs.add(link_iid)
                    self.add_link(self.mr_issue_link_dict, link_iid, issue.iid)
                # add links and mentions to dictionary as well
                self.issue_mr_link_dict[issue.iid] = linked_mrs
                self.issue_mr_mention_dict[issue.iid] = mentioned_mrs
                # create event log for create issue event
                self.add_event(issue.id, self.action_prefix + '_issue_created',
                               issue.created_at, case_id, issue.author['id'], issue.author['name'], case_id,
                               issue.issue_type, '', str(self.project_id))
                # closed event
                if issue.closed_at is not None:
                    # issue.closed_by is a method which does something else
                    # see https://github.com/python-gitlab/python-gitlab/issues/2590
                    self.add_event(issue.id, self.action_prefix + '_issue_closed', issue.closed_at, case_id,
                                   issue.asdict()['closed_by']['id'], issue.asdict()['closed_by']['name'], case_id,
                                   '', '', str(self.project_id))
                # Try to find an external issue id. description can be null
                if issue.description is not None:
                    ext_issue_id = self.find_ext_issue_id(issue.title + ' ' + issue.description)
                else:
                    ext_issue_id = self.find_ext_issue_id(issue.title)
                # id is the global id, iid is project specific id
                issue_dict = {'id': issue.id, 'iid': issue.iid, 'title': issue.title, 'author_id': issue.author['id'],
                              'created_time': issue.created_at, 'type': issue.issue_type,
                              'updated_time': issue.updated_at, 'state': issue.state, 'project_id': issue.project_id,
                              'ext_issue_id': ext_issue_id, 'linked_mrs': linked_mrs, 'mentioned_mrs': mentioned_mrs}
                self.issue_list.append(issue_dict)
                self.log_status(issue_counter, len(issues))
            except (TypeError, KeyError):
                self.logger.error('Error occurred retrieving data for: ' + str(issue.iid) + ' moving to next.')
                traceback.print_exc()
            self.logger.reset_prefix()
        self.logger.info('number of issue related events found: ' + str(self.added_event_count()))
        return self.event_logs

