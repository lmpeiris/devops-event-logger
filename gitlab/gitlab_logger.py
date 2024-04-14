import pandas as pd
from GitlabConnector import GitlabConnector
import json

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

# ======= start of code ===============
# ----- running code ------------
# initialize global var
event_logs = []
issue_list = []
mr_list = []
user_dict = {}
# iterate over project ids - as generally single 'project' has multiple gitlab 'projects'
# you can get project id by going to project id page and click on right hand side context menu
for project_id in gitlab_project_id_list:
    glc = GitlabConnector(gitlab_base_url, gitlab_private_token, project_id, external_issue_ref_regex)
    events = glc.get_all_events(production_run)
    event_logs.extend(events)
    issue_list.extend(glc.issue_list)
    mr_list.extend(glc.mr_list)
    # dictionary merge
    user_dict = {**user_dict, **glc.user_ref}
print('====== Saving data======')
# converting to pandas dataframes
event_df = pd.DataFrame(event_logs)
# use pm4py.format_dataframe and then pm4py.convert_to_event_log to convert this to an event log
# please use utils/process_mining.py for this task
issue_df = pd.DataFrame(issue_list)
mr_df = pd.DataFrame(mr_list)
print(event_df.info())
print(issue_df.info())
print(mr_df.info())
# if getting errors here, install pyarrow
event_df.to_parquet(parquet_file, compression='gzip')
issue_df.to_parquet('gitlab_issues.parquet.gz', compression='gzip')
mr_df.to_parquet('gitlab_mrs.parquet.gz', compression='gzip')
# dump user data
json_file = open(user_json_dump, "w")
json.dump(user_dict, json_file)


