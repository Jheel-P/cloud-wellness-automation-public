# import modules
from pprint import pprint
import sys
import datetime
import gspread
import requests
import pandas as pd
import traceback
from gspread_dataframe import set_with_dataframe
from google.cloud.monitoring_v3.types.metric_service import ListTimeSeriesRequest
from google.protobuf import duration_pb2 as duration
from google.protobuf import timestamp_pb2 as timestamp
from google.cloud import monitoring_v3
import variables

def iam():

    # set module variables
    project_id = variables.project_id
    # bq_job_project_id = variables.bq_job_project_id
    # bq_dataset_project_id = variables.bq_dataset_project_id
    gsheet_url = variables.sheet_url
    sa_auth_period = variables.sa_auth_period
    sa_key_auth_period = variables.sa_key_auth_period
    sa_key_expiry_period = variables.sa_key_expiry_period
    now = datetime.datetime.utcnow()

    # build iam client
    from googleapiclient import discovery
    monitoring_client = monitoring_v3.MetricServiceClient(credentials=variables.credentials)
    iam_client = discovery.build('iam', 'v1', credentials=variables.credentials)
    gspread_client = gspread.authorize(variables.credentials)
    gsheet = gspread_client.open_by_url(gsheet_url)

    high_level_roles = ['roles/editor', 'roles/owner', 'roles/resourcemanager.projectIamAdmin']

    service_accounts_list = []
    project_iam_policies_list = []
    service_accounts_keys_list = []
    service_accounts_iam_policy_list = []
    active_sa = []
    active_sa_key = []
    service_accounts_creation_logs = []
    iam_service_accounts = []
    iam_users = []
    table_export = []

    try:
        # get service accounts list
        name = f'projects/{project_id}'
        request = iam_client.projects().serviceAccounts().list(name=name)
        while True:
            response = request.execute()
            for service_account in response.get('accounts', []):
                service_accounts_list.append(service_account)
            request = iam_client.projects().serviceAccounts().list_next(previous_request=request, previous_response=response)
            if request is None:
                break
        for sa in service_accounts_list :
            if sa['email'].split('.')[-3] == 'iam' :
                sa['userManaged'] = True
            else :
                sa['userManaged'] = False
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # get project level iam policies
        resource = project_id
        url = f"https://cloudresourcemanager.googleapis.com/v1/projects/{resource}:getIamPolicy"
        response = requests.post(url, headers=variables.oauth_header).json()
        if 'bindings' in response :
            project_iam_policies_list = response['bindings']
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # get service account keys list
        for service_account in service_accounts_list :
            name = f'projects/{project_id}/serviceAccounts/{service_account["uniqueId"]}'  # TODO: Update placeholder value.
            request = iam_client.projects().serviceAccounts().keys().list(name=name)
            response = request.execute()
            for key in response.get('keys', []) :
                key["serviceAccount"] = service_account["email"]
                service_accounts_keys_list.append(key)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # get service account iam policies
        for service_account in service_accounts_list :
            service_account['iamPolicies'] = []
            resource = f'projects/{project_id}/serviceAccounts/{service_account["uniqueId"]}'  # TODO: Update placeholder value.
            request = iam_client.projects().serviceAccounts().getIamPolicy(resource=resource)
            response = request.execute()
            if 'bindings' in response :
                bindings = {
                    "serviceAccount" : service_account["email"],
                    "bindings" : response.get('bindings', [])
                }
                service_accounts_iam_policy_list.append(bindings)
                service_account['iamPolicies'] = response.get('bindings', [])
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # get service account and key auth events
        target_project = project_id
        end = datetime.datetime.utcnow()
        start = now - datetime.timedelta(days=sa_auth_period)
        end_time = timestamp.Timestamp(seconds=int(end.timestamp()))
        start_time = timestamp.Timestamp(seconds=int(start.timestamp()))
        interval = monitoring_v3.types.TimeInterval(
            end_time=end_time, start_time=start_time)
        aggregation = monitoring_v3.types.Aggregation(
            alignment_period=duration.Duration(seconds=300),
            per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_COUNT,
            cross_series_reducer=monitoring_v3.Aggregation.Reducer.REDUCE_NONE,
            group_by_fields = ['resource.labels."unique_id"', 'resource.labels."project_id"'],
        )

        filter = 'metric.type="iam.googleapis.com/service_account/authn_events_count" AND resource.type="iam_service_account"'
        req = ListTimeSeriesRequest(
            name=f"projects/{target_project}",
            filter=filter,
            interval=interval,
            view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            aggregation=aggregation,
        )

        sa_auth_events = monitoring_client.list_time_series(req)
        for item in sa_auth_events:
            if item.resource.labels not in active_sa:
                active_sa.append(item.resource.labels)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # get service account and key auth events
        aggregation = monitoring_v3.types.Aggregation(
            alignment_period=duration.Duration(seconds=300),
            per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_COUNT,
            cross_series_reducer=monitoring_v3.Aggregation.Reducer.REDUCE_NONE,
            group_by_fields = ['resource.labels."unique_id"', 'resource.labels."project_id"', 'metric.labels."key_id"'],
        )

        filter = 'metric.type="iam.googleapis.com/service_account/key/authn_events_count" AND resource.type="iam_service_account"'
        req = ListTimeSeriesRequest(
            name=f"projects/{target_project}",
            filter=filter,
            interval=interval,
            view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            aggregation=aggregation,
        )

        sa_key_auth_events = monitoring_client.list_time_series(req)
        for item in sa_key_auth_events: 
            if ({**item.resource.labels,**item.metric.labels}) not in active_sa_key:
                active_sa_key.append({**item.resource.labels,**item.metric.labels})
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # get service account creation logs
        time_period = (now - datetime.timedelta(days=sa_auth_period)).strftime("%Y-%m-%dT%H:%M:%SZ")
        timestamp_period = 'timestamp >= "' + time_period + '"'
        log_name ="resource.type=service_account"
        method_str = "protoPayload.methodName=google.iam.admin.v1.CreateServiceAccount"
        project_label = "resource.labels.project_id=" + project_id
        filter = timestamp_period + " AND " + log_name + " AND " + method_str + " AND " + project_label + ' AND severity="NOTICE"'
        resource_names = ['projects/' + project_id]
        import json
        body = {
            "resourceNames": resource_names,
            "filter": filter,
            "orderBy": "timestamp asc"
        }
        url = 'https://logging.googleapis.com/v2/entries:list'
        response = requests.post(url, headers=variables.oauth_header, json=body)
        while True :
            if 'entries' in response.json() :
                service_accounts_creation_logs = service_accounts_creation_logs + (response.json()['entries'])
            if 'nextPageToken' in response.json() :
                page_token = response.json()['nextPageToken']
                body = {
                    "resourceNames": resource_names,
                    "filter": filter,
                    "orderBy": "timestamp asc",
                    "pageToken": page_token
                }
                response = requests.post(url, headers=variables.oauth_header, json=body)
            else :
                break
    except:
        print(traceback.format_exc())
        sys.stdout.flush()
    
    try:
        # preprocess service accounts data
        service_accounts_dict = {}
        for sa in service_accounts_list :
            if sa['email'].split('.')[-3] == 'iam' :
                sa['userManaged'] = True
            else :
                sa['userManaged'] = False
            sa['mature'] = True
            sa['roles'] = []
            sa['active'] = False
            sa['highLevelRoles'] = []
            sa['impersonation'] = []
            service_accounts_dict[sa['email']] = sa

        for entry in service_accounts_creation_logs :
            if "response" in entry["protoPayload"] :
                if entry["protoPayload"]["response"]["email"] in service_accounts_dict :
                    service_accounts_dict[entry["protoPayload"]["response"]["email"]]['mature'] = False
                    service_accounts_dict[entry["protoPayload"]["response"]["email"]]['active'] = True

        for entry in project_iam_policies_list :
            for principal in entry["members"] :
                if principal.split(":")[0] == "serviceAccount" :
                    if principal.split(':')[1] in service_accounts_dict :
                        service_accounts_dict[principal.split(':')[1]]['roles'].append(entry["role"])

        for key, entry in service_accounts_dict.items() :
            for role in entry['roles']:
                if role in high_level_roles:
                    entry['highLevelRoles'].append(role)

        for entry in active_sa :
            for sa, value in service_accounts_dict.items() :
                if entry["unique_id"] == value["uniqueId"] :
                    value['active'] = True
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # preprocess iam users data
        iam_users_dict = {}
        for entry in project_iam_policies_list :
            for principal in entry["members"] :
                if principal.split(":")[0] == "user" :
                    if principal.split(":")[1] not in iam_users_dict :
                        temp_dict = {
                            'user' : principal.split(":")[1],
                            'roles' : [],
                            'highLevelRoles': [],
                            'impersonation': []
                        }
                        temp_dict['roles'].append(entry["role"])
                        iam_users_dict[principal.split(":")[1]] = temp_dict
                    else :
                        iam_users_dict[principal.split(":")[1]]['roles'].append(entry["role"])

        for entry in service_accounts_iam_policy_list :
            for binding in entry['bindings']:
                if binding['role'] == 'roles/iam.serviceAccountUser':
                    for member in binding['members']:
                        if member.split(':')[0] == 'user':
                            if member.split(':')[-1] in iam_users_dict:
                                iam_users_dict[member.split(':')[-1]]['impersonation'].append(entry['serviceAccount'])
                        if member.split(':')[0] == 'serviceAccount':
                            if member.split(':')[-1] in service_accounts_dict:
                                service_accounts_dict[member.split(':')[-1]]['impersonation'].append(entry['serviceAccount'])

        for key, entry in iam_users_dict.items() :
            for role in entry['roles']:
                if role in high_level_roles:
                    entry['highLevelRoles'].append(role)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # preprocess service account keys
        for key in service_accounts_keys_list :
            key['active'] = False
            key['mature'] = False
            key['expired'] = False
            if (now - datetime.datetime.strptime(key["validAfterTime"], "%Y-%m-%dT%H:%M:%SZ")).days >= sa_key_auth_period :
                key['mature'] = True
                key['active'] = True
            if (now - datetime.datetime.strptime(key["validAfterTime"], "%Y-%m-%dT%H:%M:%SZ")).days >= sa_key_expiry_period :
                key['expired'] = True
            for entry in active_sa_key :
                if entry["key_id"] == key["name"].split('/')[-1] :
                    key['active'] = True
                    break

        for key, value in service_accounts_dict.items() :
            iam_service_accounts.append(value)
        for key, value in iam_users_dict.items() :
            iam_users.append(value)
        iam_service_account_keys = service_accounts_keys_list
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # export service accounts assessment to sheet
        for item in iam_service_accounts :
            temp_dict = {
                "Project ID": item['projectId'],
                "Entity Type": "Service Account",
                "Entity": item['email'],
                "Parent": f"projects/{item['projectId']}",
                "Impersonation": '',
                "Roles": '',
                "High Level Roles": '',
                "Project Owner": False,
                "User Managed": item['userManaged'],
                "Active": item['active'],
                "Expired": 'NA',
                "Reqiured Permissions": '',
                "Suggested Roles": '',
                "Required Action": '',
            }
            if 'roles/owner' in item['roles']:
                temp_dict['Project Owner'] = True
            text = ''''''
            for sa in item['impersonation']:
                text = f"{text}- {sa}\n"
            text = text[:-1]
            temp_dict['Impersonation'] = text
            text = ''''''
            for role in item['roles']:
                text = f"{text}- {role}\n"
            text = text[:-1]
            temp_dict['Roles'] = text
            text = ''''''
            for role in item['highLevelRoles']:
                text = f"{text}- {role}\n"
            text = text[:-1]
            temp_dict['High Level Roles'] = text
            table_export.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        for item in iam_service_account_keys :
            temp_dict = {
                "Project ID": item['name'].split('/')[1],
                "Entity Type": "Service Account Key",
                "Entity": item['name'].split('/')[-1],
                "Parent": f"{item['serviceAccount']}",
                "Impersonation": 'NA',
                "Roles": '',
                "High Level Roles": '',
                "Project Owner": False,
                "User Managed": True,
                "Active": item['active'],
                "Expired": item['expired'],
                "Required Permissions": 'NA',
                "Suggested Roles": 'NA',
                "Required Action": '',
            }
            if 'roles/owner' in service_accounts_dict[item['serviceAccount']]['roles']:
                temp_dict['Project Owner'] = True
            if item['keyType'] == 'SYSTEM_MANAGED':
                temp_dict['User Managed'] = False
            else:
                temp_dict['User Managed'] = True
            text = ''''''
            for role in service_accounts_dict[item['serviceAccount']]['roles']:
                text = f"{text}- {role}\n"
            text = text[:-1]
            temp_dict['Roles'] = text
            text = ''''''
            for role in service_accounts_dict[item['serviceAccount']]['highLevelRoles']:
                text = f"{text}- {role}\n"
            text = text[:-1]
            temp_dict['High Level Roles'] = text
            table_export.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    if len(table_export) > 0:
        try:
            gsheet.add_worksheet(title='IAM_SERVICE_ACCOUNTS', rows=0, cols=0)
        except:
            print(traceback.format_exc())
            sys.stdout.flush()
        ws = gsheet.worksheet("IAM_SERVICE_ACCOUNTS")
        df = pd.DataFrame(table_export)
        df = df[['Project ID', 'Entity Type', 'Entity', 'Parent', 'Impersonation', 'Roles', 'High Level Roles', 'Project Owner', 
                'User Managed', 'Active', 'Expired', 'Required Permissions', 'Suggested Roles', 'Required Action']]
        set_with_dataframe(ws, df)

    table_export = []
    try:
        # export iam users assessment to sheet
        for item in iam_users :
            temp_dict = {
                "Project ID": project_id,
                "Entity Type": "User",
                "Entity": item['user'],
                "Parent": f"projects/{project_id}",
                "Impersonation": '',
                "Roles": '',
                "High Level Roles": '',
                "Project Owner": False,
                "Required Permissions": '',
                "Suggested Roles": '',
                "Required Action": '',
            }
            if 'roles/owner' in item['roles']:
                temp_dict['Project Owner'] = True
            text = ''''''
            for sa in item['impersonation']:
                text = f"{text}- {sa}\n"
            text = text[:-1]
            temp_dict['Impersonation'] = text
            text = ''''''
            for role in item['roles']:
                text = f"{text}- {role}\n"
            text = text[:-1]
            temp_dict['Roles'] = text
            text = ''''''
            for role in item['highLevelRoles']:
                text = f"{text}- {role}\n"
            text = text[:-1]
            temp_dict['High Level Roles'] = text
            table_export.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    if len(table_export) > 0:
        try:
            gsheet.add_worksheet(title='IAM_USERS', rows=0, cols=0)
        except:
            print(traceback.format_exc())
            sys.stdout.flush()
        ws = gsheet.worksheet("IAM_USERS")
        df = pd.DataFrame(table_export)
        df = df[['Project ID', 'Entity Type', 'Entity', 'Parent', 'Impersonation', 'Roles', 'High Level Roles', 'Project Owner', 'Required Permissions', 'Suggested Roles', 'Required Action']]
        set_with_dataframe(ws, df)