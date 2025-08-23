import pandas as pd
from GitlabConnector import GitlabConnector
import json
import csv
import logging.config
import os
import sys
sys.path.insert(0, '../common')
from LMPUtils import LMPUtils


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
    # converting to pandas dataframes
    event_df = pd.DataFrame(event_logs)
    # convert event log to datetime
    event_df['time'] = LMPUtils.iso_to_datetime64(event_df['time'], preserve_timezone)
    print(event_df)
    # use pm4py.format_dataframe and then pm4py.convert_to_event_log to convert this to an event log
    # please use utils/process_mining.py for this task
    issue_df = pd.DataFrame(issue_list)
    for i in ['created_time', 'updated_time']:
        issue_df[i] = LMPUtils.iso_to_datetime64(issue_df[i], preserve_timezone)
    mr_df = pd.DataFrame(mr_list)
    for i in ['created_time', 'updated_time']:
        mr_df[i] = LMPUtils.iso_to_datetime64(mr_df[i], preserve_timezone)
    commit_df = pd.DataFrame(commit_list)
    commit_df['created_time'] = LMPUtils.iso_to_datetime64(commit_df['created_time'], preserve_timezone)
    # dump pipeline data
    pl_df = pd.DataFrame(pl_list)
    for i in ['created_time', 'updated_time']:
        pl_df[i] = LMPUtils.iso_to_datetime64(pl_df[i], preserve_timezone)
    logger.info('====== event summary ======')
    logger.info(event_df.info())
    logger.info('====== issue summary ======')
    logger.info(issue_df.info())
    logger.info('====== MR summary ======')
    logger.info(mr_df.info())
    logger.info('====== commit summary ======')
    logger.info(commit_df.info())
    logger.info('====== pipeline summary ======')
    logger.info(pl_df.info())
    # if getting errors here, install pyarrow
    event_df.to_parquet('gitlab_event_logs_' + parquet_suffix + '.parquet.gz', compression='gzip')
    issue_df.to_parquet('gitlab_issues_' + parquet_suffix + '.parquet.gz', compression='gzip')
    mr_df.to_parquet('gitlab_mrs_' + parquet_suffix + '.parquet.gz', compression='gzip')
    commit_df.to_parquet('gitlab_commits_' + parquet_suffix + '.parquet.gz', compression='gzip')
    pl_df.to_parquet('gitlab_pipelines_' + parquet_suffix + '.parquet.gz', compression='gzip')
    # dump user data
    json_file = open(user_json_dump, "w")
    json.dump(user_dict, json_file)

