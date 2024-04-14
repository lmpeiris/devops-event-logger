# from gitlab.v4.objects import ProjectIssue
import gitlab
import re
import traceback


class GitlabConnector:
    def __init__(self, base_url: str, pvt_token: str, project_id: int, ext_issue_ref_regex: str):
        print('[INFO] Running test auth to: ' + base_url)
        self.gl = gitlab.Gitlab(base_url, private_token=pvt_token)
        self.gl.auth()
        self.project_id = project_id
        self.project_object = self.gl.projects.get(self.project_id)
        print('==================================================================')
        print('[INFO] Project id is: ' + str(project_id) + ' path is: ' + self.project_object.path_with_namespace)
        self.event_counter = 0
        # -- dicts for fast ref
        # input - issue iid, provides various info
        self.issue_iid_dict = {}
        # input mr iid, out - issue iid
        self.mr_iid_dict = {}
        # input - commit hash, out - case id
        self.commit_case_dict = {}
        # input - user (gitlab id or email), out - user ref
        self.user_ref = {}
        # --- lists for entity data dumping
        self.issue_list = []
        self.mr_list = []
        self.pl_list = []
        self.ext_issue_regex = re.compile(ext_issue_ref_regex)
        self.event_logs = []

    def add_event(self, event_id, action, time, case, user, user_ref, info1: str = '', info2: str = ''):
        fields_ok = True
        # carry out None checks
        for i in event_id, action, time, case, user, user_ref:
            if i is None:
                fields_ok = False
        if fields_ok:
            # TODO: enable data privacy setting to encrypt these info. email as key should be hashed when passed,
            #  this function will only hash user_ref
            # adding to dump later as a user reference from all events
            self.user_ref[str(user)] = str(user_ref)
            self.event_logs.append({'id': str(event_id), 'action': str(action),
                                    'time': str(time), 'case': str(case),
                                    'user': str(user), 'info1': info1, 'info2': info2})

            self.event_counter += 1

    def get_all_events(self, prod_run: bool = False) -> list[dict]:
        event_logs = []
        events = self.get_issues_events(prod_run)
        event_logs.extend(events)
        events = self.get_mrs_events(prod_run)
        event_logs.extend(events)
        events = self.get_pipeline_events(prod_run)
        event_logs.extend(events)
        return event_logs

    def find_ext_issue_id(self, input_text: str) -> str:
        # we are considering the first match only
        result = self.ext_issue_regex.search(input_text)
        if result is None:
            return ''
        else:
            match = result.group(1)
            print('[DEBUG] found reference to external issue id: ' + match)
            return match

    def generate_case_id(self, value, prefix_type: str = 'issue') -> str:
        prefix = ''
        match prefix_type:
            case 'issue':
                prefix = 'GLI'
            case 'mr':
                prefix = 'MR'
            case 'pipeline':
                prefix = 'GLPL'
        return prefix + '-' + str(self.project_id) + '-' + str(value)

    def get_pipeline_events(self, prod_run: bool = False) -> list[dict]:
        self.event_logs = []
        project = self.project_object
        pipelines = project.pipelines.list(get_all=prod_run)
        print('[INFO] number of pipelines found for project: ' + str(len(pipelines)))
        for pipeline in pipelines:
            try:
                # For pipelines, standard is to use global id
                print('[DEBUG] reading data for pipeline: ' + str(pipeline.id))
                # Pulling pl again due to https://python-gitlab.readthedocs.io/en/v4.4.0/faq.html#attribute-error-list
                pl = project.pipelines.get(pipeline.id)
                if pl.sha in self.commit_case_dict:
                    case_id = self.commit_case_dict[pl.sha]
                else:
                    print('[WARN] unable to find issue for pipeline: ' + str(pl.id))
                    # pl.iid can be null, hence avoid using it for PL altogether
                    case_id = self.generate_case_id(pl.id, 'pipeline')
                # TODO: create the PL list
                # pipeline created event
                self.add_event(pl.id, 'gl_PL_created',  pl.created_at, case_id, pl.user['id'], pl.user['name'])
                # pipeline completed event - only adds if finished_at is not none
                self.add_event(pl.id, 'gl_PL_completed', pl.finished_at, case_id, pl.user['id'], pl.user['name'])
                # get pipeline jobs
                jobs = pl.jobs.list(get_all=prod_run)
                print('[DEBUG] jobs found for pipeline: ' + str(len(jobs)))
                # TODO: better strategy would be to find when the first job of each stage started,
                #  and have one event per stage
                for job in jobs:
                    if job.status in ['started', 'failed', 'success']:
                        # add job event
                        # job started at time could be None - as created jobs may not have run
                        self.add_event(job.id, 'gl_job_started', job.started_at, case_id, job.user['id'],
                                       job.user['name'], str(job.name), str(job.stage))
            except (TypeError, KeyError):
                print('[ERROR] Error occurred retrieving data for: ' + str(pipeline.id) + ' moving to next.')
                traceback.print_exc()
        print('[INFO] number of pipeline related events found: ' + str(len(self.event_logs)))
        return self.event_logs

    def get_mrs_events(self, prod_run: bool = False) -> list[dict]:
        print('[INFO] scanning MRs in project_id: ' + str(self.project_id))
        self.event_logs = []
        project = self.project_object
        merge_requests = project.mergerequests.list(get_all=prod_run)
        print('[INFO] number of MRs found for project: ' + str(len(merge_requests)))
        for mr in merge_requests:
            try:
                print('[DEBUG] reading data for MR: ' + str(mr.iid))
                if mr.iid in self.mr_iid_dict:
                    case_id = self.generate_case_id(self.mr_iid_dict[mr.iid], 'issue')
                else:
                    print('[WARN] unable to find issue for MR: ' + str(mr.iid))
                    case_id = self.generate_case_id(mr.iid, 'mr')
                # create event log for create MR event
                # TODO: use a map to get the user email later
                self.add_event(mr.id, 'gl_MR_created', mr.created_at, case_id, mr.author['id'], mr.author['name'])
                if mr.merged_at is not None:
                    # adding merged event
                    # it was observed that sometimes merge user id not defined - skip them
                    if 'id' in mr.merge_user:
                        self.add_event(mr.id, 'gl_MR_merged', mr.merged_at,
                                       case_id, mr.merge_user['id'], mr.merge_user['name'])
                # add closing event, if mr.closed_at is not None
                self.add_event(mr.id, 'gl_MR_closed', mr.closed_at,
                               case_id, mr.closed_by['id'], mr.closed_by['name'])
                # primary commit should be added to pipeline dict, replace but give a warning
                if mr.sha in self.commit_case_dict:
                    print('[WARN] setting ' + str(mr.iid) + ' as MR for ' + mr.sha + ' previous linked MR was '
                          + self.commit_case_dict[mr.sha])
                self.commit_case_dict[mr.sha] = case_id
                # post-merge pipelines can run from merged commit
                if mr.merge_commit_sha is not None:
                    # but do not overwrite
                    # TODO: find out the true link or introduce a priority system with ranking
                    if mr.merge_commit_sha not in self.commit_case_dict:
                        self.commit_case_dict[mr.merge_commit_sha] = case_id
                # find commit events
                # TODO: commits which are not allocated to a MR or squashed will not be found
                commits = mr.commits()
                print('[DEBUG] commits found related to MR: ' + str(len(commits)))
                for commit in commits:
                    # NOTE: commit do not provide gitlab user id, but provides email
                    self.add_event(commit.id, 'gl_commit', commit.created_at,
                                   case_id, commit.author_email, commit.author_name)
                    # merge pipelines could be launched from any commit related to MR
                    # but do not overwrite
                    if commit.id not in self.commit_case_dict:
                        self.commit_case_dict[commit.id] = case_id
                # Try to find an external issue id. description can be null
                if mr.description is not None:
                    ext_issue_id = self.find_ext_issue_id(mr.title + ' ' + mr.description)
                else:
                    ext_issue_id = self.find_ext_issue_id(mr.title)
                # add entry to MR dict
                mr_dict = {'id': mr.id, 'iid': mr.iid, 'title': mr.title, 'author_id': mr.author['id'],
                           'author_name': mr.author['name'], 'created_time': mr.created_at,
                           'updated_time': mr.updated_at, 'state': mr.state, 'source_branch': mr.source_branch,
                           'target_branch': mr.target_branch, 'project_id': mr.project_id, 'ext_issue_id': ext_issue_id}
                self.mr_list.append(mr_dict)
            except (TypeError, KeyError):
                print('[ERROR] Error occurred retrieving data for: ' + str(mr.iid) + ' moving to next.')
                traceback.print_exc()
        print('[INFO] number of MR related events found: ' + str(len(self.event_logs)))
        return self.event_logs

    def get_issues_events(self, prod_run: bool = False) -> list[dict]:
        print('[INFO] scanning issues in project_id: ' + str(self.project_id))
        # --initialising values---
        self.event_logs = []
        branch_create_regex = re.compile('created branch')
        assigned_regex = re.compile('assigned to')
        mr_regex = re.compile('mentioned in merge request')
        # ----------
        project = self.project_object
        issues = project.issues.list(get_all=prod_run)
        print('[INFO] number of issues found for project: ' + str(len(issues)))
        for issue in issues:
            try:
                print('[DEBUG] reading data for issue: ' + str(issue.iid))
                # update internal reference dict
                self.issue_iid_dict[issue.iid] = {}
                self.issue_iid_dict[issue.iid]['id'] = issue.id
                self.issue_iid_dict[issue.iid]['branches'] = []
                case_id = self.generate_case_id(issue.iid, 'issue')
                # read through notes to find assign events and branch creation
                notes = issue.notes.list(get_all=prod_run)
                print('[DEBUG] notes found for issue: ' + str(len(notes)))
                for note in notes:
                    # TODO: issue comments are not supported yet
                    # check whether there's assigned note
                    if re.search(assigned_regex, note.body) is not None:
                        # add assigned event
                        self.add_event(note.id, 'gl_issue_assigned',
                                       note.created_at, case_id, note.author['id'], note.author['name'], note.body)
                    # check whether there's branch creation
                    if re.search(branch_create_regex, note.body) is not None:
                        # get branch name as string
                        issue_branch = note.body.split('`')[1]
                        self.issue_iid_dict[issue.iid]['branches'].append(issue_branch)
                        # add branch create event
                        self.add_event(note.id, 'gl_branch_created',
                                       note.created_at, case_id, note.author['id'], note.author['name'], issue_branch)
                    # identify any merge requests linked
                    if re.search(mr_regex, note.body) is not None:
                        # get MR iid and add as int
                        issue_mr = int(note.body.split('!')[1])
                        # TODO: this assumes MR binds to only one issue. need to fix?
                        if issue_mr in self.mr_iid_dict:
                            print('[WARN] MR ' + str(issue_mr) + ' is already added to issue ' +
                                  str(self.mr_iid_dict[issue_mr]))
                        else:
                            self.mr_iid_dict[issue_mr] = issue.iid

                # create event log for create issue event
                # TODO: use user email to map. for now, use gitlab id or email as available
                self.add_event(issue.id, 'gl_issue_created',
                               issue.created_at, case_id, issue.author['id'], issue.author['name'])
                # closed event - if issue.closed_at is not None
                self.add_event(issue.id, 'gl_issue_closed', issue.closed_at, case_id,
                               issue.closed_by['id'], issue.closed_by['name'])
                # Try to find an external issue id. description can be null
                if issue.description is not None:
                    ext_issue_id = self.find_ext_issue_id(issue.title + ' ' + issue.description)
                else:
                    ext_issue_id = self.find_ext_issue_id(issue.title)
                # id is the global id, iid is project specific id
                issue_dict = {'id': issue.id, 'iid': issue.iid, 'title': issue.title, 'author_id': issue.author['id'],
                              'author_name': issue.author['name'], 'created_time': issue.created_at,
                              'updated_time': issue.updated_at, 'state': issue.state, 'project_id': issue.project_id,
                              'ext_issue_id': ext_issue_id}
                self.issue_list.append(issue_dict)
            except (TypeError, KeyError):
                print('[ERROR] Error occurred retrieving data for: ' + str(issue.iid) + ' moving to next.')
                traceback.print_exc()
        print('[INFO] number of issue related events found: ' + str(len(self.event_logs)))
        return self.event_logs

    # def get_issue_events(self, issue_object: ProjectIssue):
    #     state_events_list = issue_object.resourcestateevents.list(target_type='merge_request')
    # def find_issue_for_mr(self, source_branch: str, mr_description: str) -> int:
    #     """ Attempts to find an issue id for MR, if failed returns 0"""
    #     match = self.issue_regex.search(mr_description).group(1)
    #     found_iid = int(match.split('#')[1])
    #     max_issue_iid = max(self.issue_iid_dict)
    #     if max_issue_iid <= found_iid:
    #         return found_iid
    #     else:
    #         try:
    #             found_iid = int(source_branch.split('-'[0]))
    #             if max_issue_iid <= found_iid:
    #                 return found_iid
    #             else:
    #                 found_iid = 0
    #         except TypeError:
    #             found_iid = 0
    #     return found_iid