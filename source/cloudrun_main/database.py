# import modules
import sys
import datetime
import gspread
import requests
import traceback
import pandas as pd
from statistics import mean, pstdev
from gspread_dataframe import set_with_dataframe
from google.cloud.monitoring_v3.types.metric_service import ListTimeSeriesRequest
from google.protobuf import duration_pb2 as duration
from google.protobuf import timestamp_pb2 as timestamp
from google.cloud import monitoring_v3
import variables


def database():
    # set module variables
    project_id = variables.project_id
    bq_job_project_id = variables.bq_job_project_id
    bq_dataset_project_id = variables.bq_dataset_project_id
    gsheet_url = variables.sheet_url
    monitor_period = 30             # days
    now = datetime.datetime.utcnow()

    monitoring_client = monitoring_v3.MetricServiceClient(credentials=variables.credentials)
    gspread_client = gspread.authorize(variables.credentials)
    gsheet = gspread_client.open_by_url(gsheet_url)

    cloudsql_instances = []
    cloudsql_tiers = []
    sql_instance_cpu_utilization = []
    table_export = []

    try:
        headers = dict(variables.oauth_header)
        url = f"https://sqladmin.googleapis.com/v1/projects/{project_id}/instances"
        params = {}
        while True:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                if 'items' in response.json():
                    cloudsql_instances += response.json()['items']
            if 'nextPageToken' in response.json():
                params = {
                    'pageToken': response.json()['nextPageToken']
                }
            else:
                break
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        headers = dict(variables.oauth_header)
        url = f"https://sqladmin.googleapis.com/v1/projects/{project_id}/tiers"
        params = {}
        while True:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                if 'items' in response.json():
                    cloudsql_tiers += response.json()['items']
            if 'nextPageToken' in response.json():
                params = {
                    'pageToken': response.json()['nextPageToken']
                }
            else:
                break
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    target_project = project_id
    end = datetime.datetime.utcnow()
    start = now - datetime.timedelta(days=monitor_period)
    end_time = timestamp.Timestamp(seconds=int(end.timestamp()))
    start_time = timestamp.Timestamp(seconds=int(start.timestamp()))
    interval = monitoring_v3.types.TimeInterval(end_time=end_time, start_time=start_time)
    aggregation = monitoring_v3.types.Aggregation(
        alignment_period=duration.Duration(seconds=300),
        per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
        cross_series_reducer=monitoring_v3.Aggregation.Reducer.REDUCE_NONE,
        group_by_fields = ['resource.labels."project_id"', 'resource.labels."database_id"', 'resource.labels."region"', 'metadata.labels."name"']
    )

    try:
        # retrieve and process cpu and memory metrics of instances
        filter = 'metric.type="cloudsql.googleapis.com/database/cpu/utilization" AND resource.type="cloudsql_database"'
        req = ListTimeSeriesRequest(
            name=f"projects/{target_project}",
            filter=filter,
            interval=interval,
            view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            aggregation=aggregation,
        )
        sql_instance_results = monitoring_client.list_time_series(req)

        for item in sql_instance_results:
            temp_list = []
            for point in item.points:
                temp_list.append(point.value.double_value)
            append_dict = {
                'max': max(temp_list),
                'min': min(temp_list),
                'mean': mean(temp_list),
                'stddev': pstdev(temp_list),
                'database': item.resource.labels['database_id'].split(':')[-1],
                'projectId': item.resource.labels['project_id'],
                'databaseId': item.resource.labels['database_id'],
                'uptime': len(temp_list)*5/60
            }
            sql_instance_cpu_utilization.append(append_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # retrieve and process cpu and memory metrics of instances
        filter = 'metric.type="cloudsql.googleapis.com/database/memory/utilization" AND resource.type="cloudsql_database"'
        req = ListTimeSeriesRequest(
            name=f"projects/{target_project}",
            filter=filter,
            interval=interval,
            view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            aggregation=aggregation,
        )
        sql_instance_results = monitoring_client.list_time_series(req)
        sql_instance_memory_utilization = []

        for item in sql_instance_results:
            temp_list = []
            for point in item.points:
                temp_list.append(point.value.double_value)
            append_dict = {
                'max': max(temp_list),
                'min': min(temp_list),
                'mean': mean(temp_list),
                'stddev': pstdev(temp_list),
                'database': item.resource.labels['database_id'].split(':')[-1],
                'projectId': item.resource.labels['project_id'],
                'databaseId': item.resource.labels['database_id'],
                'uptime': len(temp_list)*5/60
            }
            sql_instance_memory_utilization.append(append_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()
    
    machine_type_exclusion = ['db-f1-micro', 'db-g1-small']
    for instance in cloudsql_instances:
        ans = False
        for tier in cloudsql_tiers:
            if instance['settings'].get('tier') == tier['tier']:
                if tier['tier'] not in machine_type_exclusion:
                    instance['cpu'] = tier['tier'].split('-')[-1]
                    instance['memoryGb'] = tier['RAM']/(1024*1024)
                else:
                    instance['cpu'] = 1
                    instance['memoryGb'] = tier['RAM']/(1024*1024)
                ans = True
                break
        if not ans:
            instance['cpu'] = int(instance['settings'].get('tier').split('-')[-2])
            instance['memoryGb'] = int(instance['settings'].get('tier').split('-')[-1])/1024
    
    try:
        for instance in cloudsql_instances:
            temp_dict = {
                'Project ID': f'{project_id}',
                'Entity Type': 'Cloud SQL Instance',
                'Entity': instance['name'],
                'Parent': instance.get('masterInstanceName', f'Project: {project_id}').split('/')[-1],
                'Region': instance['region'],
                'Zone': instance['gceZone'],
                'Instance Type': instance.get('instanceType', ''),
                'Master Instance': instance.get('masterInstanceName', 'NA'),
                'Availability': instance['settings'].get('availabilityType', ''),
                'Database Version': instance.get('databaseVersion', ''),
                'Backup': instance['settings'].get('backupConfiguration', {}).get('enabled', False),
                'IP Addresses': '',
                'Public IP': False,
                'Disk': f"{instance['settings'].get('dataDiskType', '')}: {instance['settings'].get('dataDiskSizeGb', '')} GB",
                'Machine Type': instance['settings'].get('tier'),
                'CPU': instance.get('cpu', ''),
                'Memory (GB)': instance.get('memoryGb', ''),
                'Uptime': '',
                'P95 CPU Utilization': '',
                'P95 Memory Utilization': '',
                'Required Action': ''
            }
            for item in sql_instance_cpu_utilization:
                if item['database'] == temp_dict['Entity']:
                    temp_dict['Uptime'] = round(item['uptime'], 2)
                    temp_dict['P95 CPU Utilization'] = round((item['mean'] + 2*item['stddev'])*100, 2)
                    break
            for item in sql_instance_memory_utilization:
                if item['database'] == temp_dict['Entity']:
                    temp_dict['P95 Memory Utilization'] = round((item['mean'] + 2*item['stddev'])*100, 2)
                    break 
            text = ''''''
            for ip in instance.get('ipAddresses', []):
                text = f"{text}- {ip.get('type', '')}: {ip.get('ipAddress')}\n"
                if ip.get('type', '') != 'PRIVATE':
                    temp_dict['Public IP'] = True
            if text != '''''':
                text = text[:-1]
            temp_dict['IP Addresses'] = text
            table_export.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    if len(table_export) > 0:
        try:
            gsheet.add_worksheet(title='CLOUD_SQL_INSTANCES', rows=0, cols=0)
        except:
            print(traceback.format_exc())
            sys.stdout.flush()
        ws = gsheet.worksheet("CLOUD_SQL_INSTANCES")
        df = pd.DataFrame(table_export)
        df = df[['Project ID', 'Entity Type', 'Entity', 'Parent', 'Region', 'Zone', 'Instance Type', 'Master Instance', 
                'Availability', 'Database Version', 'Backup', 'IP Addresses', 'Public IP', 'Disk', 'Machine Type', 'CPU',
                'Memory (GB)', 'Uptime', 'P95 CPU Utilization', 'P95 Memory Utilization', 'Required Action']]
        set_with_dataframe(ws, df)