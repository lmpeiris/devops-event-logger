import re
import traceback
from DevOpsConnector import DevOpsConnector


class ALMConnector(DevOpsConnector):
    def __init__(self, namespace: str, ext_issue_ref_regex: str, api_delay: int, case_type_prefixes: dict):
        DevOpsConnector.__init__(self, namespace, api_delay)
        self.case_type_prefixes = case_type_prefixes
        self.action_prefix = self.case_type_prefixes['action_prefix']
        # this should be overwritten by subclasses
        self.project_id = 0
        self.ext_issue_regex = re.compile(ext_issue_ref_regex)
        # -------------------------------------------
        # -- dicts for fast ref
        # -------------------------------------------
        self.mr_issue_link_dict = {}
        self.mr_issue_mention_dict = {}
        self.issue_created_dict = {}
        self.mr_case_id = {}
        self.mr_created_dict = {}
        self.commit_mr_pre_merge_dict = {}
        self.commit_mr_post_merge_dict = {}
        self.commit_info = {}
        self.commit_mr_commits_dict = {}
        self.commit_case_id = {}
        self.mr_list = []
        self.commit_list = []
        self.pl_list = []

    def analyse_commit_events(self) -> list[dict]:
        """Analyses commit events already read from various sources and admits them as events"""
        self.logger.info('analysing commit list for project id: ' + str(self.project_id))
        # iterate through the mr commits dict which contains already read commits
        for commit_sha, commit in self.commit_info.items():
            try:
                case_id, link_type, mr_iid = self.find_case_id_for_commit(commit_sha)
                self.commit_case_id[commit_sha] = case_id
                local_case = self.generate_case_id(commit_sha[:6], 'commit')
                action = self.action_prefix + '_commit'
                self.add_event(commit_sha, action, commit['time'], case_id, commit['user'],
                               commit['user_ref'], local_case, commit['info1'], '', str(self.project_id))
                commit_dict = {'id': commit_sha, 'author': commit['user'], 'created_time': commit['time'],
                               'case_id': case_id, 'project_id': self.project_id,
                               'pre_merge': self.empty_set_or_value(self.commit_mr_pre_merge_dict, commit_sha),
                               'post_merge': self.empty_set_or_value(self.commit_mr_post_merge_dict, commit_sha),
                               'commit_list': self.empty_set_or_value(self.commit_mr_commits_dict, commit_sha),
                               'chosen_mr': mr_iid}
                self.commit_list.append(commit_dict)
            except (TypeError, KeyError):
                self.logger.error('Error occurred retrieving data for: ' + str(commit_sha) + ' moving to next.')
                traceback.print_exc()
        self.logger.info('number of commit events found: ' + str(self.added_event_count()))
        return self.event_logs

    def generate_case_id(self, value, prefix_type: str) -> str:
        """Case id will be generated according to case_type_prefixes"""
        prefix = ''
        if prefix_type in self.case_type_prefixes:
            prefix = self.case_type_prefixes[prefix_type]
        return prefix + '-' + str(self.project_id) + '-' + str(value)

    def find_case_id_for_pl(self, pl_sha: str, pl_id) -> str:
        """Get the case id for a given pipeline.
        Provides issue related id if links are found, else provides local scope"""
        if pl_sha in self.commit_case_id:
            case_id = self.commit_case_id[pl_sha]
        else:
            # TODO: if this hits frequently we may have to implement specific commit pull here
            self.logger.warn('did not find a relation to a commit for pipeline: ' + str(pl_id))
            case_id = self.generate_case_id(pl_id, 'pipeline')
        return case_id

    def find_case_id_for_commit(self, commit_sha: str) -> tuple[str, str, str]:
        """ Get case id for a commit event. Gives case_id, link_type, mr_iid as tuple"""
        pre_merge = self.commit_mr_pre_merge_dict
        post_merge = self.commit_mr_post_merge_dict
        commit_list = self.commit_mr_commits_dict
        mr_iid = ''
        if (commit_sha in pre_merge) and len(pre_merge[commit_sha]) > 0:
            mr_iid = self.get_max_timed_id(pre_merge[commit_sha], self.mr_created_dict)
            case_id = self.mr_case_id[mr_iid]
            link_type = 'pre_merge'
        elif (commit_sha in post_merge) and len(post_merge[commit_sha]) > 0:
            mr_iid = self.get_max_timed_id(post_merge[commit_sha], self.mr_created_dict)
            case_id = self.mr_case_id[mr_iid]
            link_type = 'post_merge'
        elif (commit_sha in commit_list) and len(commit_list[commit_sha]) > 0:
            mr_iid = self.get_max_timed_id(commit_list[commit_sha], self.mr_created_dict)
            case_id = self.mr_case_id[mr_iid]
            link_type = 'commit_related'
        else:
            # generate case id using first 6 digits of sha
            case_id = self.generate_case_id(commit_sha[:6], 'commit')
            link_type = 'undefined'
            self.logger.warn('did not find a relation to an MR for commit: ' + str(commit_sha))
        return case_id, link_type, str(mr_iid)

    def find_case_id_for_mr(self, mr_str: str) -> tuple[str, str]:
        """Get the case id for a given MR. Returns case_id, link_type as tuple"""
        linked = self.mr_issue_link_dict
        mentioned = self.mr_issue_mention_dict
        if (mr_str in linked) and len(linked[mr_str]) > 0:
            latest_issue = self.get_max_timed_id(linked[mr_str], self.issue_created_dict)
            case_id = self.generate_case_id(latest_issue, 'issue')
            link_type = 'mr_link'
        elif (mr_str in mentioned) and len(mentioned[mr_str]) == 1:
            issue_iid = next(iter(mentioned[mr_str]))
            self.logger.warn('linking MR to issue using mentions: ' + mr_str)
            case_id = self.generate_case_id(issue_iid, 'issue')
            link_type = 'mr_mention'
        else:
            self.logger.warn('no relation found to an issue for MR : ' + mr_str)
            case_id = self.generate_case_id(mr_str, 'mr')
            link_type = 'undefined'
        return case_id, link_type

    def find_ext_issue_id(self, input_text: str) -> str:
        """Find external system issued ticket id using regex"""
        # we are considering the first match only
        result = self.ext_issue_regex.search(input_text)
        if result is None:
            return ''
        else:
            match = result.group(1)
            self.logger.debug('found reference to external issue id: ' + match)
            return match
