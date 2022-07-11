# import modules
import sys
import datetime
import gspread
import pandas as pd
import traceback
from statistics import mean, pstdev
from gspread_dataframe import set_with_dataframe
from google.cloud.monitoring_v3.types.metric_service import ListTimeSeriesRequest
from google.protobuf import duration_pb2 as duration
from google.protobuf import timestamp_pb2 as timestamp
from google.cloud import monitoring_v3
import variables


def kubernetes_engine():
    # set module variables
    project_id = variables.project_id
    bq_job_project_id = variables.bq_job_project_id
    bq_dataset_project_id = variables.bq_dataset_project_id
    gsheet_url = variables.sheet_url
    monitor_period = 30             # days
    now = datetime.datetime.utcnow()

    from googleapiclient import discovery
    gce_service = discovery.build('compute', 'v1', credentials=variables.credentials)
    gke_service = discovery.build('container', 'v1', credentials=variables.credentials)
    monitoring_client = monitoring_v3.MetricServiceClient(credentials=variables.credentials)
    gspread_client = gspread.authorize(variables.credentials)
    gsheet = gspread_client.open_by_url(gsheet_url)

    gke_clusters = []
    gce_migs = []
    gce_igs = []
    gke_nodepools_cpu_metric = []
    gke_nodepools_memory_metric = []
    gke_node_pools = []

    try:
        # get list of gke clusters
        parent = f'projects/{project_id}/locations/-'  # TODO: Update placeholder value.
        request = gke_service.projects().locations().clusters().list(parent=parent)
        response = request.execute()
        gke_clusters = response.get('clusters', [])
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # get list of managed instance groups
        request = gce_service.instanceGroupManagers().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for name, instance_group_managers_scoped_list in response['items'].items():
                gce_migs += instance_group_managers_scoped_list.get('instanceGroupManagers', [])
            request = gce_service.instanceGroupManagers().aggregatedList_next(previous_request=request, previous_response=response)
        for mig in gce_migs :
            mig['instanceCount'] = 0
            for version in mig['versions'] :
                mig['instanceCount'] += version['targetSize']['calculated']

        request = gce_service.instanceGroups().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for name, instance_groups_scoped_list in response['items'].items():
                gce_igs += instance_groups_scoped_list.get('instanceGroups', [])
            request = gce_service.instanceGroups().aggregatedList_next(previous_request=request, previous_response=response)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # get cpu allocatable utilization
        target_project = project_id
        end = datetime.datetime.utcnow()
        start = now - datetime.timedelta(days=monitor_period)
        end_time = timestamp.Timestamp(seconds=int(end.timestamp()))
        start_time = timestamp.Timestamp(seconds=int(start.timestamp()))
        interval = monitoring_v3.types.TimeInterval(end_time=end_time, start_time=start_time)
        filter = 'metric.type="kubernetes.io/node/cpu/allocatable_utilization"'

        aggregation = monitoring_v3.types.Aggregation(
            alignment_period = duration.Duration(seconds=300),
            per_series_aligner = monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            cross_series_reducer = monitoring_v3.Aggregation.Reducer.REDUCE_MEAN,
            group_by_fields = ['metadata.user_labels."cloud.google.com/gke-nodepool"', 'resource.labels."cluster_name"'],
        )

        req = ListTimeSeriesRequest(
            name = f"projects/{target_project}",
            filter = filter,
            interval = interval,
            view = monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            aggregation = aggregation,
        )

        results = monitoring_client.list_time_series(req)
        for item in results :
            temp_dict = {
                'labels' : item.resource.labels,
                'nodepool' : item.metadata.user_labels['cloud.google.com/gke-nodepool'],
                'points' : []
            }
            for point in item.points :
                temp_dict['points'].append({
                    "timestamp" : point.interval.start_time,
                    "value" : point.value.double_value
                })
            gke_nodepools_cpu_metric.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # get memory allocatable utilization
        filter = 'metric.type="kubernetes.io/node/memory/allocatable_utilization"'

        aggregation = monitoring_v3.types.Aggregation(
            alignment_period = duration.Duration(seconds=300),
            per_series_aligner = monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            cross_series_reducer = monitoring_v3.Aggregation.Reducer.REDUCE_MEAN,
            group_by_fields = ['metadata.user_labels."cloud.google.com/gke-nodepool"', 'resource.labels."cluster_name"'],
        )

        req = ListTimeSeriesRequest(
            name = f"projects/{target_project}",
            filter = filter,
            interval = interval,
            view = monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            aggregation = aggregation,
        )

        results = monitoring_client.list_time_series(req)
        for item in results :
            temp_dict = {
                'labels' : item.resource.labels,
                'nodepool' : item.metadata.user_labels['cloud.google.com/gke-nodepool'],
                'points' : []
            }
            for point in item.points :
                temp_dict['points'].append({
                    "timestamp" : point.interval.start_time,
                    "value" : point.value.double_value
                })
            gke_nodepools_memory_metric.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        for cluster in gke_clusters :
            for nodepool in cluster['nodePools'] :
                for entry in gke_nodepools_memory_metric :
                    if entry['labels']['cluster_name'] == cluster['name'] :
                        if entry['nodepool'] == nodepool['name'] :
                            temp_list = []
                            for point in entry['points'] :
                                temp_list.append(point['value'])
                            nodepool['memoryMetrics'] = {
                                'uptime' : len(temp_list) * 5,
                                'mean' : mean(temp_list),
                                'std' : pstdev(temp_list)
                            }
                            break
            for entry in gke_nodepools_cpu_metric :
                if entry['labels']['cluster_name'] == cluster['name'] :
                    if entry['nodepool'] == nodepool['name'] :
                        temp_list = []
                        for point in entry['points'] :
                                temp_list.append(point['value'])
                        nodepool['cpuMetrics'] = {
                                'uptime' : len(temp_list) * 5,
                                'mean' : mean(temp_list),
                                'std' : pstdev(temp_list)
                        }
                        break
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        for cluster in gke_clusters:
            for nodepool in cluster.get('nodePools', []):
                temp_dict = {
                    'Project ID': project_id,
                    'Entity Type': 'GKE Node Pool',
                    'Entity': nodepool['name'],
                    'Parent': cluster['name'],
                    'Location': cluster.get('location'),
                    'Zone': 'NA',
                    'Node Pool Locations': '',
                    'Node Count': 0,
                    'Machine Type': nodepool.get('config', {}).get('machineType'),
                    'Machine Series': '',
                    'CPU': '',
                    'Memory (GB)': '',
                    'Extended Memory (GB)': '',
                    'Disk': f"{nodepool.get('config', {}).get('diskType', '')}: {nodepool.get('config', {}).get('diskSizeGb', '')} GB",
                    'Image': nodepool.get('config', {}).get('imageType'),
                    'Autoscaling': False,
                    'Service Account': nodepool.get('config', {}).get('serviceAccount'),
                    'Access Scopes': 'Default/Custom',
                    'Auto Repair': nodepool.get('management', {}).get('autoRepair', False),
                    'Auto Upgrade': nodepool.get('management', {}).get('autoUpgrade', False),
                    'P95 CPU Allocatable Utilization': 'NA',
                    'P95 Memory Allocatable Utilization': 'NA',
                    'Required Action': ''
                }
                if 'cpuMetrics' in nodepool:
                    temp_dict['P95 CPU Allocatable Utilization'] = (nodepool['cpuMetrics']['mean'] + 2*nodepool['cpuMetrics']['std'])*100
                if 'memoryMetrics' in nodepool:
                    temp_dict['P95 Memory Allocatable Utilization'] = (nodepool['memoryMetrics']['mean'] + 2*nodepool['memoryMetrics']['std'])*100
                for ig in gce_igs:
                    for mig in nodepool.get('instanceGroupUrls', []):
                        if ig['name'] == mig.split('/')[-1]:
                            temp_dict['Node Count'] += ig.get('size', 0)
                            break
                if 'https://www.googleapis.com/auth/cloud-platform' in nodepool.get('config', {}).get('oauthScopes', []):
                    temp_dict['Access Scopes'] = 'Full'
                if nodepool.get('autoscaling', {}).get('enabled', False):
                    temp_dict['Autoscaling'] = f"{nodepool.get('autoscaling', {}).get('minNodeCount', 0)}-{nodepool.get('autoscaling', {}).get('maxNodeCount')}"
                text = ''''''
                for location in nodepool.get('locations', []):
                    text = f"{text}- {location}\n"
                if text != '''''':
                    text = text[:-1]
                temp_dict['Node Pool Locations'] = text
                gke_node_pools.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    if len(gke_node_pools) > 0:
        try:
            gsheet.add_worksheet(title='GKE_NODE_POOLS', rows=0, cols=0)
        except:
            print(traceback.format_exc())
            sys.stdout.flush()
        ws = gsheet.worksheet("GKE_NODE_POOLS")
        df = pd.DataFrame(gke_node_pools)
        df = df[['Project ID', 'Entity Type', 'Entity', 'Parent', 'Location', 'Zone', 'Node Pool Locations', 'Node Count', 'Machine Type',
                'Machine Series', 'CPU', 'Memory (GB)', 'Extended Memory (GB)', 'Disk', 'Image', 'Autoscaling', 'Service Account',
                'Access Scopes', 'Auto Repair', 'Auto Upgrade', 'P95 CPU Allocatable Utilization', 'P95 Memory Allocatable Utilization',
                'Required Action']]
        set_with_dataframe(ws, df)