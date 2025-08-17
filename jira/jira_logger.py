import os
import pandas as pd
from jiraConnector import JiraConnector
import json
import logging.config
import xmltodict
import sys
sys.path.insert(0, '../common')
from LMPUtils import LMPUtils

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
    # TODO: by default jira can only export 1000 issues to csv. Ok for projects with less than that
    #  You may use time query to generate multiple csvs and combine
    #  or use python-jira https://jira.readthedocs.io/api.html#jira.client.JIRA.search_issues

    # parquet file suffix to use for saving dataframe; compressed in gz
    parquet_suffix = os.environ['JIRA_PARQUET_SUFFIX']
    # Note: reporter id is jira id. For now, do not expose via environment var
    jira_issue_columns = settings['issue_columns']
    # delay in seconds between api calls for issues
    issue_api_delay = float(os.environ['JIRA_API_DELAY'])
    # false is good for a test run, with only partial data is retrieved
    production_run = LMPUtils.env_bool('JIRA_PRODUCTION_RUN')
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

    # initialize global var
    event_logs = []
    user_info_dict = {}
    # initialize jira connector
    logger.info('======== Jira api calls starting : ===========')
    jira_connector = JiraConnector(jira_url, auth_token, 'default', auth_email)
    jira_connector.user_ref = user_info_dict

    # load jira issues
    issue_df = pd.DataFrame()
    issue_count = 0
    if jira_issue_source_type == 'xml':
        # TODO: both get issue api and xml provide reliable comment list. Get the info from there
        jira_issue_source = settings['issue_source']['path']
        with open(jira_issue_source) as xml_source:
            jira_xml = xmltodict.parse(xml_source.read())
            jira_connector.iterate_xml_issues(jira_xml, production_run)
    else:
        jira_project_key = os.environ['JIRA_PRJ_KEY']
        jira_issue_start = int(os.environ['JIRA_START_KEY'])
        jira_issue_end = int(os.environ['JIRA_STOP_KEY'])
        issue_key_list = []
        for i in range(jira_issue_start, jira_issue_end + 1):
            issue_key_list.append(jira_project_key + '-' + str(i))
        logger.info('Number of issues to be read: ' + str(len(issue_key_list)))
        for issue_key in issue_key_list:
            jira_connector.get_issue_via_api(issue_key)

    issue_df = pd.DataFrame(jira_connector.issue_list)
    # remove any missing values with 'na' in parent field
    issue_df['parent'] = issue_df['parent'].fillna('na')
    # convert time fields accordingly. We will be using datetime64[ns] throughout, without tz info for performance
    # if using more than one timezone convert before stripping
    issue_df['created'] = pd.to_datetime(issue_df['created'], format='ISO8601').dt.tz_localize(None)
    logger.info('======== Loaded issues: ===========')
    logger.info(issue_df.info)

    # create event df
    event_df = pd.DataFrame(jira_connector.event_logs)
    event_df['time'] = pd.to_datetime(event_df['time'], format='ISO8601').dt.tz_localize(None)
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

