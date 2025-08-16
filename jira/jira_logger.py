import os
import pandas as pd
import time
from jiraConnector import JiraConnector
import json
import logging.config
import xmltodict


if __name__ == '__main__':
    # ===== configurations ===============
    # read main config
    with open('../common/settings.json', 'r') as settings_file:
        settings = json.load(settings_file)['jira']
    # Export jira issue csv with all fields using jira UI itself
    # Note: put 'r' prefix below for windows paths when using absolute path.
    # WARNING: CSV export is known to skip tracks in fields when csv is decoded. Using xml output is recommended
    # when running via container, need to mount the folder
    jira_issue_source_type = settings['issue_source']['type']
    jira_issue_source = settings['issue_source']['path']
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
    issue_df = pd.DataFrame()
    # strftime conversion, see https://strftime.org/
    time_format = '%d/%b/%y %I:%M %p'
    if jira_issue_source_type == 'csv':
        issue_df = pd.read_csv(jira_issue_source, index_col='Issue key', usecols=jira_issue_columns)
    else:
        # TODO: both get issue api and xml provide reliable comment list. Get the info from there
        with open(jira_issue_source) as xml_source:
            jira_xml = xmltodict.parse(xml_source.read())
        issue_list = []
        issue_index = []
        for issue in jira_xml['rss']['channel']['item']:
            if 'parent' in issue:
                parent = issue['parent']['#text']
            else:
                parent = 'na'
            issue_index.append(issue['key']['#text'])
            if 'timespent' in issue:
                timespent = int(issue['timespent']['@seconds'])
            else:
                timespent = 0
            issue_list.append({'Reporter Id': issue['reporter']['@accountid'], 'Reporter': issue['reporter']['#text'],
                               'Issue Type': issue['type']['#text'], 'Parent': parent,
                               'Issue id': issue['key']['@id'], 'Created': issue['created'],
                               'Project key': issue['project']['@key'], 'timespent': timespent})
        issue_df = pd.DataFrame(issue_list, index=issue_index)
        time_format = '%a, %d %b %Y %H:%M:%S %z'
    # testing with small dataset
    # issue_df = issue_df.iloc[0:2]

    issue_df['Created'] = pd.to_datetime(issue_df['Created'], format=time_format)
    logger.info('======== Loaded issues: ===========')
    logger.info(issue_df.info)

    # initialize global var
    event_logs = []
    user_info_dict = {}
    # initialize jira connector
    logger.info('======== Jira api calls starting : ===========')
    jira_connector = JiraConnector(jira_url, auth_token, 'default', auth_email)
    jira_connector.user_ref = user_info_dict
    jira_connector.iterate_issues(issue_df, settings['issue_df_column_mapping'])
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

