import os
from google.auth import default
from google.auth.transport.requests import Request

project_id = ""
bq_job_project_id = os.environ.get('BQ_JOB_PROJECT_ID')
bq_dataset_project_id = os.environ.get('BQ_DATASET_PROJECT_ID')
template_sheet = os.environ.get('TEMPLATE_SHEET_ID')
topic_id = os.environ.get('PUBSUB_TOPIC_ID')
kind = os.environ.get('DATASTORE_KIND')

# initialize credentials and generate oauth tokens
credentials, project = default(quota_project_id=bq_job_project_id, scopes=["https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file"])
credentials.refresh(Request())
oauth_token = credentials.token
oauth_header = {
    "Authorization": f"Bearer {oauth_token}"
}

