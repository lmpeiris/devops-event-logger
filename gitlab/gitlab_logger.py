import pandas as pd
from GitlabConnector import GitlabConnector
import json
import csv
import logging
import logging.config


def load_user_email_map(file_path_to_file: str, target_dict: dict):
    logger.info('opening gitlab id email map file: ' + file_path_to_file)
    with open(file_path_to_file, 'r') as user_file:
        reader = csv.reader(user_file, delimiter=',')
        for row in reader:
            # 1 - email, 0 - gitlab id
            target_dict[row[1]] = row[0]


if __name__ == '__main__':
    # ===== configurations ============
    gitlab_base_url = 'http://xxxxx.net'
    gitlab_private_token = 'xxxxxxxxxxx'
    # data will be pulled from the mentioned project id list
    gitlab_project_id_list = [431, 270, 71, 5137, 5186, 4936, 4941, 4821, 5079, 4398, 4500, 4816]
    # regex for identifying jira ticket id mentions
    external_issue_ref_regex = '(ABCD-+[0-9(_)]+)'
    # when enabled, data will be retrieved via apis completely.
    # false is good for a test run, with only partial data is retrieved
    production_run = True

    # parquet to save the event_logs dataframe; compressed in gz
    parquet_file = 'gitlab_event_logs.parquet.gz'
    # file to save user information as json
    user_json_dump = 'found_users.json'
    # file to load gitlab user id to email mapping
    # keep empty if not going to be used
    gitlab_id_email_csv = ''

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
    load_user_email_map(gitlab_id_email_csv, user_email_map)
    for project_id in gitlab_project_id_list:
        glc = GitlabConnector(gitlab_base_url, gitlab_private_token, project_id, external_issue_ref_regex)
        if gitlab_id_email_csv != '':
            glc.user_email_map = user_email_map
        events = glc.get_all_events(production_run)
        event_logs.extend(events)
        issue_list.extend(glc.issue_list)
        mr_list.extend(glc.mr_list)
        pl_list.extend(glc.pl_list)
        commit_list.extend(glc.commit_list)
        # dictionary merge
        user_dict = {**user_dict, **glc.user_ref}
    logger.info('====== Saving data======')
    # converting to pandas dataframes
    event_df = pd.DataFrame(event_logs)
    # convert event log to datetime
    event_df['time'] = pd.to_datetime(event_df['time'], utc=True)
    # use pm4py.format_dataframe and then pm4py.convert_to_event_log to convert this to an event log
    # please use utils/process_mining.py for this task
    issue_df = pd.DataFrame(issue_list)
    for i in ['created_time', 'updated_time']:
        issue_df[i] = pd.to_datetime(issue_df[i], utc=True)
    mr_df = pd.DataFrame(mr_list)
    for i in ['created_time', 'updated_time']:
        mr_df[i] = pd.to_datetime(mr_df[i], utc=True)
    commit_df = pd.DataFrame(commit_list)
    commit_df['created_time'] = pd.to_datetime(commit_df['created_time'], utc=True)
    # dump pipeline data
    pl_df = pd.DataFrame(pl_list)
    for i in ['created_time', 'updated_time']:
        pl_df[i] = pd.to_datetime(pl_df[i], utc=True)
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
    event_df.to_parquet(parquet_file, compression='gzip')
    issue_df.to_parquet('gitlab_issues.parquet.gz', compression='gzip')
    mr_df.to_parquet('gitlab_mrs.parquet.gz', compression='gzip')
    commit_df.to_parquet('gitlab_commits.parquet.gz', compression='gzip')
    pl_df.to_parquet('gitlab_pipelines.parquet.gz', compression='gzip')
    # dump user data
    json_file = open(user_json_dump, "w")
    json.dump(user_dict, json_file)

