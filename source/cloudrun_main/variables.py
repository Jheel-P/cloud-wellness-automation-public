import os
import datetime
from google.auth import default
from google.auth.transport.requests import Request

project_id = ""
bq_job_project_id = os.environ.get('BQ_JOB_PROJECT_ID')
bq_dataset_project_id = os.environ.get('BQ_DATASET_PROJECT_ID')
sheet_url = ""
kind = os.environ.get('DATASTORE_KIND')

monitor_period = 30                                
sa_auth_period = 120
sa_key_auth_period = 30
sa_key_expiry_period = 120
now = datetime.datetime.utcnow()

# initialize credentials and generate oauth tokens
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./credentials.json"
credentials, project = default(quota_project_id=bq_job_project_id, scopes=["https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file"])
credentials.refresh(Request())
oauth_token = credentials.token
oauth_header = {
    "Authorization": f"Bearer {oauth_token}"
}

