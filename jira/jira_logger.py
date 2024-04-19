import pandas as pd
import time
from jiraConnector import JiraConnector

if __name__ == '__main__':
    # ===== configurations ===============
    # Export jira issue csv with all fields using jira UI itself
    # Note: put 'r' prefix below for windows paths when using absolute path.
    # Not needed for linux paths or relative paths
    jira_issue_csv = r'C:\Users\malshan\Documents\log-collector\JIRAOld.csv'
    # TODO: by default jira can only export 1000 issues to csv. Ok for projects with less than that
    #  You may use time query to generate multiple csvs and combine
    #  or use python-jira https://jira.readthedocs.io/api.html#jira.client.JIRA.search_issues

    # parquet to save the event_logs dataframe; compressed in gz
    parquet_file = 'jira_event_logs_2.parquet.gz'
    # Note: reporter id is jira id
    # 'Summary' excluded as too long
    jira_issue_columns = ['Issue key', 'Issue id', 'Reporter Id', 'Created', 'Updated', 'Resolved', 'Affects versions', 'Parent']
    # delay between api calls for issues
    issue_api_delay = 1

    # jira instance and authentication
    jira_url = "https://xxxxxx.atlassian.net"
    auth_token = "xxxxxx"
    auth_email = "abc@xxx"

    # ======= start of code =============
    # load jira issues
    issue_df = pd.read_csv(jira_issue_csv, index_col='Issue key', usecols=jira_issue_columns)

    # testing with small dataset
    # issue_df = issue_df.iloc[0:2]

    # set proper time format
    for i in ['Created', 'Updated', 'Resolved']:
        issue_df[i] = pd.to_datetime(issue_df[i], format='%d/%b/%y %I:%M %p')
    print('======== Loaded issues: ===========')
    print(issue_df.info)

    # initialize global var
    event_logs = []
    user_info_dict = {}
    # initialize jira connector
    print('======== Jira api calls starting : ===========')
    issue_count = 0
    jira_api = JiraConnector(jira_url, auth_token, auth_email)
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
        created_event = {'id': str(row['Issue id']), 'title': '', 'action': 'jira_created', 'user': user_email,
                         'time': row['Created'], 'case': jira_issue_key}
        event_logs.append(created_event)
        # get events from changelog
        change_logs = jira_api.get_change_log(jira_issue_key)
        event_logs.extend(change_logs)
        # get comment events
        comment_events = jira_api.get_comments(jira_issue_key)
        event_logs.extend(comment_events)
        cur_progress = str(len(event_logs))
        print('[INFO] Events found so far ' + str(len(event_logs)) + ', issues completed: ' + str(issue_count))
    # create df
    event_df = pd.DataFrame(event_logs)
    # use pm4py.format_dataframe and then pm4py.convert_to_event_log to convert this to an event log
    # please use utils/process_mining.py for this task
    print('======== Event log data: ===========')
    print(event_df.info())
    # if getting errors here, install pyarrow
    event_df.to_parquet(parquet_file, compression='gzip')

