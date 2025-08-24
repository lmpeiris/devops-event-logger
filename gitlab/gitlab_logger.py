import pandas as pd
from GitlabConnector import GitlabConnector
import json
import csv
import logging.config
import os
import sys
sys.path.insert(0, '../common')
from LMPUtils import LMPUtils
from DevOpsConnector import DevOpsConnector


def load_user_email_map(file_path_to_file: str, target_dict: dict):
    logger.info('opening gitlab id email map file: ' + file_path_to_file)
    with open(file_path_to_file, 'r') as user_file:
        reader = csv.reader(user_file, delimiter=',')
        for row in reader:
            # 1 - email, 0 - gitlab id
            target_dict[row[1]] = row[0]


if __name__ == '__main__':
    # ===== configurations ============
    # read main config
    with open('../common/settings.json', 'r') as settings_file:
        settings = json.load(settings_file)['gitlab']
    # read config from environment vars
    gitlab_base_url = os.environ['GITLAB_BASE_URL']
    gitlab_private_token = os.environ['GITLAB_PRIVATE_TOKEN']
    # data will be pulled from the mentioned project id list
    # environment var should be mentioned as comma separated list
    gitlab_project_id_list = os.environ['GITLAB_REPO_IDS'].split(',')
    # regex for identifying jira ticket id mentions
    external_issue_ref_regex = os.environ['GITLAB_EXTERNAL_ISSUE_REGEX']
    # when enabled, data will be retrieved via apis completely.
    # false is good for a test run, with only partial data is retrieved
    production_run = LMPUtils.env_bool('GITLAB_PRODUCTION_RUN')

    # parquet file suffix to use.
    parquet_suffix = os.environ['GITLAB_PARQUET_SUFFIX']
    # file to save user information as json
    user_json_dump = os.environ['GITLAB_USER_JSON_DUMP']
    # file to load gitlab user id to email mapping
    # keep value as 'None' if not going to be used
    gitlab_id_email_csv = os.environ['GITLAB_ID_EMAIL_CSV']

    # ======= start of code ===============
    # ----- running code ------------
    # initialize global var
    event_logs = []
    issue_list = []
    mr_list = []
    pl_list = []
    commit_list = []
    user_dict = {}
    user_email_map = {}
    # initialise logger
    logging.config.fileConfig('../common/logging.conf')
    logger = logging.getLogger('scriptLogger')
    # iterate over project ids - as generally single 'project' has multiple gitlab 'projects'
    # you can get project id by going to project id page and click on right hand side context menu
    if gitlab_id_email_csv != 'None':
        load_user_email_map(gitlab_id_email_csv, user_email_map)
    for project_id in gitlab_project_id_list:
        glc = GitlabConnector(gitlab_base_url, gitlab_private_token, project_id, external_issue_ref_regex)
        glc.user_email_map = user_email_map
        events = glc.get_all_events(settings['get_all_events_order'], production_run)
        event_logs.extend(events)
        issue_list.extend(glc.issue_list)
        mr_list.extend(glc.mr_list)
        pl_list.extend(glc.pl_list)
        commit_list.extend(glc.commit_list)
        # dictionary merge
        user_dict = {**user_dict, **glc.user_ref}
    logger.info('====== Saving data======')
    preserve_timezone = settings['preserve_timezone']
    # stub devops connector. this is a hack as glc connector is not available outside the loop
    devops = DevOpsConnector('gitlab', 1)
    # converting to pandas dataframes
    event_df = pd.DataFrame(event_logs)
    devops.publish_df(event_df, ['time'], preserve_timezone,  'event_logs',
                      'gitlab_event_log_' + parquet_suffix)
    # use pm4py.format_dataframe and then pm4py.convert_to_event_log to convert this to an event log
    # please use utils/process_mining.py for this task
    issue_df = pd.DataFrame(issue_list)
    devops.publish_df(issue_df, ['created_time', 'updated_time'], preserve_timezone,  'issues',
                      'gitlab_issues_' + parquet_suffix)
    mr_df = pd.DataFrame(mr_list)
    devops.publish_df(mr_df, ['created_time', 'updated_time'], preserve_timezone,
                      'merge requests', 'gitlab_MRs_' + parquet_suffix)
    commit_df = pd.DataFrame(commit_list)
    devops.publish_df(commit_df, ['created_time'], preserve_timezone,  'commits',
                      'gitlab_commits_' + parquet_suffix)
    pl_df = pd.DataFrame(pl_list)
    devops.publish_df(pl_df, ['created_time', 'updated_time'], preserve_timezone,
                      'pipelines', 'gitlab_pipelines_' + parquet_suffix)
    # dump user data
    json_file = open(user_json_dump, "w")
    json.dump(user_dict, json_file)

