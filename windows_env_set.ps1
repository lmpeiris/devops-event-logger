##### GITLAB ##########
# gitlab base url
$env:GITLAB_BASE_URL='http://xxxxx.net'
# personal access token (PAT) created in gitlab
$env:GITLAB_PRIVATE_TOKEN='xxxxxxxxxxxxx'
# comma separated repo id list
$env:GITLAB_REPO_IDS='431,270,71,5137,5186,4936,4941,4821,5079,4398,4500,4816'
# regex for identifying jira ticket id mentions
$env:GITLAB_EXTERNAL_ISSUE_REGEX='(ABCD-+[0-9(_)]+)'
# when enabled, data will be retrieved via apis completely.
# false is good for a test run, with only partial data is retrieved
$env:GITLAB_PRODUCTION_RUN='True'
# parquet file suffix to use when saving.
$env:GITLAB_PARQUET_SUFFIX='ABCD'
# file to save user information as json
$env:GITLAB_USER_JSON_DUMP='found_users.json'
# file to load gitlab user id to email mapping
# keep value as 'None' if not going to be used
$env:GITLAB_ID_EMAIL_CSV='None'


##### JIRA ##########
# parquet file suffix to use for saving dataframe; compressed in gz
$env:JIRA_PARQUET_SUFFIX='ABCD'
# delay in seconds between api calls for issues
$env:JIRA_API_DELAY='1'
# false is good for a test run, with only partial data is retrieved
$env:JIRA_PRODUCTION_RUN='True'
# save user emails to json - will be disabled if data security mode is on
$env:JIRA_USER_JSON='jira_users.json'
# jira instance and authentication
$env:JIRA_URL='https://xxxxxx.atlassian.net'
$env:JIRA_AUTH_TOKEN='xxxxxxx'
$env:JIRA_AUTH_EMAIL='abc@xxx'
# if using issue source as jira and not xml, define below
$env:JIRA_PRJ_KEY='MS'
$env:JIRA_START_KEY='5000'
$env:JIRA_STOP_KEY='5010'
