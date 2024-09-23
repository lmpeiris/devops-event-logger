import os
import pandas as pd
import time
from jiraConnector import JiraConnector
import json
import logging.config


if __name__ == '__main__':
    # ===== configurations ===============
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
    # 'Summary' excluded as too long
    jira_issue_columns = ['Issue key', 'Issue id', 'Reporter Id', 'Created', 'Updated', 'Resolved', 'Parent']
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
    issue_count = 0
    jira_api = JiraConnector(jira_url, auth_token, 'default', auth_email)
    for jira_issue_key in issue_df.index:
        issue_count = issue_count+1
        time.sleep(issue_api_delay)
        # add entry for create event
        row = issue_df.loc[jira_issue_key]
        jira_user_id = row['Reporter Id']
        # avoid using same call again
        if jira_user_id in user_info_dict:
            # for now we'll directly map the email
            user_email = user_info_dict[jira_user_id]
        else:
            user_email = jira_api.get_email_by_user_id(jira_user_id)
            user_info_dict[jira_user_id] = user_email
        # Note: id field should be kept as string object fo compatibility with hashes
        created_event = jira_api.add_event(str(row['Issue id']), 'jira_created', row['Created'], jira_issue_key,
                                           user_email, '', jira_issue_key)
        event_logs.append(created_event)
        # get events from changelog
        change_logs = jira_api.get_change_log_per_issue(jira_issue_key)
        event_logs.extend(change_logs)
        # get comment events
        comment_events = jira_api.get_comments_per_issue(jira_issue_key)
        event_logs.extend(comment_events)
        cur_progress = str(len(event_logs))
        logger.info('Events found so far ' + str(len(event_logs)) + ', issues completed: ' + str(issue_count))
    # create df
    event_df = pd.DataFrame(event_logs)
    # use pm4py.format_dataframe and then pm4py.convert_to_event_log to convert this to an event log
    # please use utils/process_mining.py for this task
    logger.info('======== Event log data: ===========')
    logger.info(event_df.info())
    # if getting errors here, install pyarrow
    event_df.to_parquet('jira_event_log_' + parquet_suffix + '.parquet.gz', compression='gzip')
    # write users to file if enabled
    # TODO: this only collects info from issue reporters - use a object property
    # if user_json is a space avoid writing user file
    if user_json != ' ':
        with open(user_json, 'w') as user_file:
            json.dump(user_info_dict, user_file)

