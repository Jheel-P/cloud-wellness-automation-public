import json
import variables
from pprint import pprint
from google.cloud import pubsub_v1
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.cloud import datastore
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

publisher = pubsub_v1.PublisherClient(credentials=variables.credentials)
topic_path = publisher.topic_path(variables.bq_job_project_id, variables.topic_id)
gauth = GoogleAuth()
scopes = ["https://www.googleapis.com/auth/cloud-platform","https://www.googleapis.com/auth/drive","https://www.googleapis.com/auth/drive.file"]
gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name('./sa_credentials.json', scopes)
datastore_client = datastore.Client(project=variables.bq_job_project_id)

def MAIN(request):

    request_json = request.get_json()
    request_json['file_name'] = f"{request_json['client']} | {request_json['project_id']} | {datetime.now().strftime('%h')} {datetime.now().year}"
    gauth.Authorize()
    drive = GoogleDrive(gauth)
    name = f"{request_json['project_id']}_iam"
    task_key = datastore_client.key(variables.kind, name)
    task = datastore_client.get(task_key)
    if task is not None:
        gsheet_url = task.get('sheet_url')
        file_id = gsheet_url.split('/')[5]
        str = "\'" + request_json['parent_folder'] + "\'" + " in parents and trashed=false"
        file_list = drive.ListFile({'q': str}).GetList()
        file_id_list = []
        for file in file_list:
            file_id_list.append(file['id'])
        if file_id in file_id_list:
            gsheet_url = task.get('sheet_url')
            print(gsheet_url)
        else:
            folder = request_json['parent_folder']
            title = request_json['file_name']
            file = variables.template_sheet
            file_attribs = drive.auth.service.files().copy(fileId=file, body={"parents": [{"kind": "drive#fileLink","id": folder}], 'title': title}).execute()
            gsheet_url = file_attribs.get('alternateLink')
            print(gsheet_url)
    else:
        folder = request_json['parent_folder']
        title = request_json['file_name']
        file = variables.template_sheet
        file_attribs = drive.auth.service.files().copy(fileId=file, body={"parents": [{"kind": "drive#fileLink","id": folder}], 'title': title}).execute()
        gsheet_url = file_attribs.get('alternateLink')
        print(gsheet_url)

    for assessment in ['iam','cost_optimization','compute_engine','compute_network','database','kubernetes_engine']:
        name = f"{request_json['project_id']}_{assessment}"
        task_key = datastore_client.key(variables.kind, name)
        task = datastore.Entity(key=task_key)
        task.update({
            'project_id': request_json['project_id'],
            'sheet_url': gsheet_url,
            'assessment': assessment,
            'status': 'REASSESS'
        })
        datastore_client.put(task)
        message = {
            "project_id": request_json['project_id'],
            "assessment": assessment,
            "sheet_url": gsheet_url
        }
        data_str = json.dumps(message)
        data = data_str.encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(future.result())

    return f"Published project {request_json['project_id']}"
