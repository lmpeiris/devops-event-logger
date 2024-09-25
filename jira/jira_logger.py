import os
import pandas as pd
import time
from jiraConnector import JiraConnector
import json
import logging.config


if __name__ == '__main__':
    # ===== configurations ===============
    # read main config
    with open('../common/settings.json', 'r') as settings_file:
        settings = json.load(settings_file)['jira']
    # Export jira issue csv with all fields using jira UI itself
    # Note: put 'r' prefix below for windows paths when using absolute path.
    # Not needed for linux paths or relative paths
    # WARNING: CSV export is known to skip tracks in fields when csv is decoded
    # when running via container, need to mount the folder
    jira_issue_csv = 'input/jira_issue.csv'
    # TODO: by default jira can only export 1000 issues to csv. Ok for projects with less than that
    #  You may use time query to generate multiple csvs and combine
    #  or use python-jira https://jira.readthedocs.io/api.html#jira.client.JIRA.search_issues

    # parquet file suffix to use for saving dataframe; compressed in gz
    parquet_suffix = os.environ['JIRA_PARQUET_SUFFIX']
    # Note: reporter id is jira id. For now, do not expose via environment var
    jira_issue_columns = settings['issue_columns']
    # delay in seconds between api calls for issues
    issue_api_delay = float(os.environ['JIRA_API_DELAY'])
    # save user emails to json - will be disabled if data security mode is on
    user_json = os.environ['JIRA_USER_JSON']

    # jira instance and authentication
    jira_url = os.environ['JIRA_URL']
    auth_token = os.environ['JIRA_AUTH_TOKEN']
    auth_email = os.environ['JIRA_AUTH_EMAIL']

    # ======= start of code =============
    # initialise logger
    logging.config.fileConfig('../common/logging.conf')
    logger = logging.getLogger('scriptLogger')
    # load jira issues
    issue_df = pd.read_csv(jira_issue_csv, index_col='Issue key', usecols=jira_issue_columns)

    # testing with small dataset
    # issue_df = issue_df.iloc[0:2]

    # set proper time format
    for i in ['Created', 'Updated', 'Resolved']:
        issue_df[i] = pd.to_datetime(issue_df[i], format='%d/%b/%y %I:%M %p')
    logger.info('======== Loaded issues: ===========')
    logger.info(issue_df.info)

    # initialize global var
    event_logs = []
    user_info_dict = {}
    # initialize jira connector
    logger.info('======== Jira api calls starting : ===========')
    jira_connector = JiraConnector(jira_url, auth_token, 'default', auth_email)
    jira_connector.user_ref = user_info_dict
    jira_connector.iterate_issues(issue_df)
    # create df
    event_df = pd.DataFrame(jira_connector.event_logs)
    # use pm4py.format_dataframe and then pm4py.convert_to_event_log to convert this to an event log
    # please use utils/process_mining.py for this task
    logger.info('======== Event log data: ===========')
    logger.info(event_df.info())
    print(event_df)
    # if getting errors here, install pyarrow
    event_df.to_parquet('jira_event_log_' + parquet_suffix + '.parquet.gz', compression='gzip')
    # write users to file if enabled
    # TODO: this only collects info from issue reporters - use a object property
    # if user_json is a space avoid writing user file
    if user_json != ' ':
        with open(user_json, 'w') as user_file:
            json.dump(user_info_dict, user_file)

