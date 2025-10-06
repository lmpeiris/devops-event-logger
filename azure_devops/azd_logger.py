import pandas as pd
from AZDConnector import AZDConnector
import json
import logging.config
import os
import sys
sys.path.insert(0, '../common')
from LMPUtils import LMPUtils
from DevOpsConnector import DevOpsConnector


if __name__ == '__main__':
    # ===== configurations ============
    # read main config
    with open('../common/settings.json', 'r') as settings_file:
        settings = json.load(settings_file)['azure_devops']
    # read config from environment vars
    AZD_base_url = os.environ['AZD_BASE_URL']
    AZD_private_token = os.environ['AZD_PRIVATE_TOKEN']
    # data will be pulled from the mentioned project id list
    # environment var should be mentioned as comma separated list
    AZD_project_id_list = os.environ['AZD_PROJECT_NAMES'].split(',')
    # regex for identifying jira ticket id mentions
    external_issue_ref_regex = os.environ['AZD_EXTERNAL_ISSUE_REGEX']
    # when enabled, data will be retrieved via apis completely.
    # false is good for a test run, with only partial data is retrieved
    production_run = LMPUtils.env_bool('AZD_PRODUCTION_RUN')

    # parquet file suffix to use.
    parquet_suffix = os.environ['AZD_PARQUET_SUFFIX']
    # file to save user information as json
    user_json_dump = os.environ['AZD_USER_JSON_DUMP']

    # ======= start of code ===============
    # ----- running code ------------
    # initialize global var
    event_logs = []
    issue_list = []
    mr_list = []
    pl_list = []
    commit_list = []
    rel_list = []
    user_dict = {}
    # initialise logger
    logging.config.fileConfig('../common/logging.conf')
    logger = logging.getLogger('scriptLogger')
    # iterate over project ids - as generally single 'project' has multiple AZD 'projects'
    # you can get project id by going to project id page and click on right hand side context menu
    for project_id in AZD_project_id_list:
        azd = AZDConnector(AZD_base_url, AZD_private_token, project_id, external_issue_ref_regex,
                           settings['case_type_prefixes'])
        events = azd.get_all_events(settings['get_all_events_order'], production_run)
        event_logs.extend(events)
        issue_list.extend(azd.issue_list)
        mr_list.extend(azd.mr_list)
        pl_list.extend(azd.pl_list)
        rel_list.extend(azd.rel_list)
        commit_list.extend(azd.commit_list)
        # dictionary merge
        user_dict = {**user_dict, **azd.user_ref}
    logger.info('====== Saving data======')
    preserve_timezone = settings['preserve_timezone']
    # stub devops connector. this is a hack as glc connector is not available outside the loop
    devops = DevOpsConnector('AZD', 1)
    # converting to pandas dataframes
    event_df = pd.DataFrame(event_logs)
    devops.publish_df(event_df, ['time'], preserve_timezone,  'event_logs',
                      'AZD_event_log_' + parquet_suffix)
    # use pm4py.format_dataframe and then pm4py.convert_to_event_log to convert this to an event log
    # please use utils/process_mining.py for this task
    issue_df = pd.DataFrame(issue_list)
    devops.publish_df(issue_df, ['created_time'], preserve_timezone,  'issues',
                      'AZD_issues_' + parquet_suffix)
    mr_df = pd.DataFrame(mr_list)
    devops.publish_df(mr_df, ['created_time'], preserve_timezone,
                      'merge requests', 'AZD_MRs_' + parquet_suffix)
    commit_df = pd.DataFrame(commit_list)
    devops.publish_df(commit_df, ['created_time'], preserve_timezone,  'commits',
                      'AZD_commits_' + parquet_suffix)
    pl_df = pd.DataFrame(pl_list)
    devops.publish_df(pl_df, ['created_time'], preserve_timezone,
                      'pipelines', 'AZD_pipelines_' + parquet_suffix)
    rel_df = pd.DataFrame(rel_list)
    devops.publish_df(rel_df, ['created_time'], preserve_timezone,
                      'releases', 'AZD_releases_' + parquet_suffix)
    # dump user data
    # TODO: there's no implementation for this to be useful yet
    json_file = open(user_json_dump, "w")
    json.dump(user_dict, json_file)

