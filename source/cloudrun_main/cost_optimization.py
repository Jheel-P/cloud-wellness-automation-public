# import modules
from pprint import pprint
import datetime
import gspread
import requests
import copy
import sys
import traceback
import pandas as pd
from statistics import mean, pstdev
from gspread_dataframe import set_with_dataframe
from google.cloud.monitoring_v3.types.metric_service import ListTimeSeriesRequest
from google.protobuf import duration_pb2 as duration
from google.protobuf import timestamp_pb2 as timestamp
from google.cloud import monitoring_v3, bigquery
import variables

def cost_optimization():
    # set module variables
    project_id = variables.project_id
    bq_job_project_id = variables.bq_job_project_id
    bq_dataset_project_id = variables.bq_dataset_project_id
    gsheet_url = variables.sheet_url
    monitor_period = 30             # days
    now = datetime.datetime.utcnow()

    # build iam client
    from googleapiclient import discovery
    monitoring_client = monitoring_v3.MetricServiceClient(credentials=variables.credentials)
    compute_service = discovery.build('compute', 'v1', credentials=variables.credentials)
    bq_client = bigquery.Client(project=bq_job_project_id)
    gspread_client = gspread.authorize(variables.credentials)
    gsheet = gspread_client.open_by_url(gsheet_url)

    query = f"SELECT * FROM `{bq_dataset_project_id}.cloud_assessment_supporting_tables.gce_machine_series`"
    query_job = bq_client.query(query)
    gce_machine_series = query_job.result()
    gce_machine_series_list = []
    gce_machine_series_dict = {}
    for row in gce_machine_series:
        gce_machine_series_dict[row['series']] = dict(row)
        gce_machine_series_list.append(dict(row))

    query = f"SELECT * FROM `{bq_dataset_project_id}.cloud_assessment_supporting_tables.gce_machine_types`"
    query_job = bq_client.query(query)
    gce_machine_types = query_job.result()
    gce_machine_types_list = []
    gce_machine_types_dict = {}
    for row in gce_machine_types:
        gce_machine_types_dict[row['name']] = dict(row)
        gce_machine_types_list.append(dict(row))

    query = "SELECT * FROM `searce-cms-production.cloud_assessment_supporting_tables.cloud_sql_pricing`"
    query_job = bq_client.query(query)
    cloud_sql_pricing = query_job.result()
    cloud_sql_pricing_list = []
    for row in cloud_sql_pricing:
        cloud_sql_pricing_list.append(dict(row))
    
    gce_zones = []
    gce_regions = []
    gce_instances = []
    gce_disks = []
    gce_disk_images = []
    gce_addresses = []
    machine_type_recommendations = []
    idle_instance_recommendations = []
    idle_disk_recommendations = []
    idle_image_recommendations = []
    idle_ip_recommendations = []
    cud_recommendations = []
    gce_cud_recommendations = []
    cloudsql_instances = []
    cloudsql_tiers = []
    sql_instance_cpu_utilization = []
    sql_instance_memory_utilization = []
    table_export = []
    table_export_2 = []
    total_savings = 0

    try:
        # get compute zones and regions
        request = compute_service.zones().list(project=project_id)
        while request is not None:
            response = request.execute()
            gce_zones += response.get('items', [])
            request = compute_service.zones().list_next(previous_request=request, previous_response=response)

        request = compute_service.regions().list(project=project_id)
        while request is not None:
            response = request.execute()
            gce_regions += response.get('items', [])
            request = compute_service.regions().list_next(previous_request=request, previous_response=response)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # get list of compute engine instances
        request = compute_service.instances().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for name, instances_scoped_list in response['items'].items():
                gce_instances += instances_scoped_list.get('instances', [])
            request = compute_service.instances().aggregatedList_next(
                previous_request=request, previous_response=response)

        # get list of compute engine disks
        request = compute_service.disks().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for name, disks_scoped_list in response['items'].items():
                gce_disks += disks_scoped_list.get('disks', [])
            request = compute_service.disks().aggregatedList_next(
                previous_request=request, previous_response=response)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # get list of disk images
        request = compute_service.images().list(project=project_id)
        while request is not None:
            response = request.execute()
            for image in response.get('items', []):
                gce_disk_images.append(image)
            request = compute_service.images().list_next(
                previous_request=request, previous_response=response)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        # get list of ip addresses
        request = compute_service.addresses().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for name, addresses_scoped_list in response['items'].items():
                if 'addresses' in addresses_scoped_list:
                    gce_addresses += addresses_scoped_list['addresses']
            request = compute_service.addresses().aggregatedList_next(
                previous_request=request, previous_response=response)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()
    
    try:
        # get machine type recommender data
        recommender = "google.compute.instance.MachineTypeRecommender"
        headers = dict(variables.oauth_header)
        headers['X-Goog-User-Project'] = bq_job_project_id
        for zone in gce_zones:
            url = f"https://recommender.googleapis.com/v1/projects/{project_id}/locations/{zone['name']}/recommenders/{recommender}/recommendations"
            params = {}
            while True:
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    if 'recommendations' in response.json():
                        machine_type_recommendations += response.json()[
                            'recommendations']
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
        # get idle instance data
        recommender = "google.compute.instance.IdleResourceRecommender"
        headers = dict(variables.oauth_header)
        headers['X-Goog-User-Project'] = bq_job_project_id
        for zone in gce_zones:
            url = f"https://recommender.googleapis.com/v1/projects/{project_id}/locations/{zone['name']}/recommenders/{recommender}/recommendations"
            params = {}
            while True:
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    if 'recommendations' in response.json():
                        idle_instance_recommendations += response.json()[
                            'recommendations']
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
        # get idle disks data
        recommender = "google.compute.disk.IdleResourceRecommender"
        headers = dict(variables.oauth_header)
        headers['X-Goog-User-Project'] = bq_job_project_id
        for zone in gce_zones:
            url = f"https://recommender.googleapis.com/v1/projects/{project_id}/locations/{zone['name']}/recommenders/{recommender}/recommendations"
            params = {}
            while True:
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    if 'recommendations' in response.json():
                        idle_disk_recommendations += response.json()[
                            'recommendations']
                if 'nextPageToken' in response.json():
                    params = {
                        'pageToken': response.json()['nextPageToken']
                    }
                else:
                    break
        for region in gce_regions:
            url = f"https://recommender.googleapis.com/v1/projects/{project_id}/locations/{region['name']}/recommenders/{recommender}/recommendations"
            params = {}
            while True:
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    if 'recommendations' in response.json():
                        idle_disk_recommendations += response.json()[
                            'recommendations']
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
        # get idle images data
        recommender = "google.compute.image.IdleResourceRecommender"
        headers = dict(variables.oauth_header)
        headers['X-Goog-User-Project'] = bq_job_project_id
        url = f"https://recommender.googleapis.com/v1/projects/{project_id}/locations/global/recommenders/{recommender}/recommendations"
        params = {}
        while True:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                if 'recommendations' in response.json():
                    idle_image_recommendations += response.json()[
                        'recommendations']
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
        # get idle ips data
        recommender = "google.compute.address.IdleResourceRecommender"
        headers = dict(variables.oauth_header)
        headers['X-Goog-User-Project'] = bq_job_project_id
        url = f"https://recommender.googleapis.com/v1/projects/{project_id}/locations/global/recommenders/{recommender}/recommendations"
        params = {}
        while True:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                if 'recommendations' in response.json():
                    idle_ip_recommendations += response.json()['recommendations']
            if 'nextPageToken' in response.json():
                params = {
                    'pageToken': response.json()['nextPageToken']
                }
            else:
                break
        for region in gce_regions:
            url = f"https://recommender.googleapis.com/v1/projects/{project_id}/locations/{region['name']}/recommenders/{recommender}/recommendations"
            params = {}
            while True:
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    if 'recommendations' in response.json():
                        idle_ip_recommendations += response.json()[
                            'recommendations']
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
        # get cud recommender data
        recommender = "google.compute.commitment.UsageCommitmentRecommender"
        headers = dict(variables.oauth_header)
        headers['X-Goog-User-Project'] = bq_job_project_id
        for region in gce_regions:
            url = f"https://recommender.googleapis.com/v1/projects/{project_id}/locations/{region['name']}/recommenders/{recommender}/recommendations"
            params = {}
            while True:
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    if 'recommendations' in response.json():
                        cud_recommendations += response.json()['recommendations']
                if 'nextPageToken' in response.json():
                    params = {
                        'pageToken': response.json()['nextPageToken']
                    }
                else:
                    break
        
        xor = {}
        for cud in cud_recommendations :
            if cud['stateInfo']['state'] == 'ACTIVE' :
                if cud['xorGroupId'] in xor :
                    for operation in cud['content']['operationGroups'][0]['operations'] :
                        if operation['action'] == "add" :
                            if cud['content']['overview']['type'] == 'MEMORY' :
                                if cud['content']['overview']['algorithm'] == 'BREAK_EVEN_POINT' :
                                    xor[cud['xorGroupId']]['BREAK_EVEN_POINT_RESOURCE_VALUE'] = operation['value']['resources'][0]['amount']/1024
                                else :
                                    xor[cud['xorGroupId']]['LOW_WATERMARK_RESOURCE_VALUE'] = operation['value']['resources'][0]['amount']/1024
                            else :
                                if cud['content']['overview']['algorithm'] == 'BREAK_EVEN_POINT' :
                                    xor[cud['xorGroupId']]['BREAK_EVEN_POINT_RESOURCE_VALUE'] = operation['value']['resources'][0]['amount']
                                else :
                                    xor[cud['xorGroupId']]['LOW_WATERMARK_RESOURCE_VALUE'] = operation['value']['resources'][0]['amount']
                            xor[cud['xorGroupId']][f"{operation['value']['plan']}_{cud['content']['overview']['algorithm']}"] = -(int(cud['primaryImpact']['costProjection']['cost'].get('units', 0)) + cud['primaryImpact']['costProjection']['cost'].get('nanos', 0)/1000000000)
                        # xor[cud['xorGroupId']][f"{operation['value']['plan']}_{cud['content']['overview']['algorithm']}"] = f"{cud['primaryImpact']['costProjection']['cost']['currencyCode']} {str(-(int(cud['primaryImpact']['costProjection']['cost']['units']) + cud['primaryImpact']['costProjection']['cost']['nanos']/1000000000))}"
                else :
                    for operation in cud['content']['operationGroups'][0]['operations'] :
                        if operation['action'] == "add" :
                            temp_dict = {
                                'projectId' : project_id,
                                'recommendation' : operation['value']['name'],
                                'machineType' : operation['value']['type'],
                                'resourceType' : operation['value']['resources'][0]['type'],
                                'region': cud['name'].split('/')[3]
                            }
                            if operation['value']['type'] == 'GENERAL_PURPOSE' :
                                    temp_dict['machineType'] = 'GENERAL_PURPOSE_N1'
                            if cud['content']['overview']['algorithm'] == 'BREAK_EVEN_POINT' :
                                    temp_dict['BREAK_EVEN_POINT_RESOURCE_VALUE'] = operation['value']['resources'][0]['amount']
                            else :
                                    temp_dict['LOW_WATERMARK_RESOURCE_VALUE'] = operation['value']['resources'][0]['amount']
                            # temp_dict[f"{operation['value']['plan']}_{cud['content']['overview']['algorithm']}"] = f"{cud['primaryImpact']['costProjection']['cost']['currencyCode']} {str(-(int(cud['primaryImpact']['costProjection']['cost']['units']) + cud['primaryImpact']['costProjection']['cost']['nanos']/1000000000))}"
                            break
                    if cud['content']['overview']['type'] == 'MEMORY' :
                        if cud['content']['overview']['algorithm'] == 'BREAK_EVEN_POINT' :
                            temp_dict['BREAK_EVEN_POINT_RESOURCE_VALUE'] = operation['value']['resources'][0]['amount']/1024
                        else :
                            temp_dict['LOW_WATERMARK_RESOURCE_VALUE'] = operation['value']['resources'][0]['amount']/1024
                    temp_dict[f"{operation['value']['plan']}_{cud['content']['overview']['algorithm']}"] = -(int(cud['primaryImpact']['costProjection']['cost'].get('units', 0)) + cud['primaryImpact']['costProjection']['cost'].get('nanos', 0)/1000000000)
                    xor[cud['xorGroupId']] = temp_dict
        for key, value in xor.items() :
            gce_cud_recommendations.append(value)
        new_cud = copy.deepcopy(gce_cud_recommendations)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        n1_shared_core_machines = [
            "g1-small",
            "f1-micro"
        ]
        for instance in gce_instances:
            instance['machineSeries'] = instance['machineType'].split('/')[-1].split('-')[0]
            if instance['machineSeries'] == 'custom' or instance['machineSeries'] in n1_shared_core_machines:
                instance['machineSeries'] = 'n1'

            if instance['machineType'].split('/')[-1] in gce_machine_types_dict:
                instance['guestCpus'] = gce_machine_types_dict[instance['machineType'].split('/')[-1]]['guestCpus']
                instance['memoryGb'] = gce_machine_types_dict[instance['machineType'].split('/')[-1]]['memoryMb']/1024
            elif instance['machineType'].split('/')[-1].find('-ext') != -1:
                instance['guestCpus'] = int(instance['machineType'].split('/')[-1].split('-')[-3])
                instance['memoryGb'] = int(instance['machineType'].split('/')[-1].split('-')[-2])/1024
                for series in gce_machine_series_list:
                    if series['series'] == instance['machineSeries'] and series['type'] != 'SHARED_CORE':
                        instance['extendedMemoryGb'] = instance['memoryGb'] - instance['guestCpus'] * series['maxMemoryPerCpu']
            elif instance['machineType'].split('/')[-1].split('-')[0] == 'e2' and instance['machineType'].split('/')[-1].split('-')[2] == 'medium':
                instance['guestCpus'] = 1
                instance['memoryGb'] = int(instance['machineType'].split('/')[-1].split('-')[-1])/1024
            else:
                instance['guestCpus'] = instance['machineType'].split('/')[-1].split('-')[-2]
                instance['memoryGb'] = int(instance['machineType'].split('/')[-1].split('-')[-1])/1024

            for item in idle_instance_recommendations:
                if item['content']['overview']['resourceName'] == instance['name']:
                    instance['isIdle'] = True
                    instance['deletionSavings'] = -(int(item['primaryImpact']['costProjection']['cost'].get('units', 0)) + item['primaryImpact']['costProjection']['cost'].get('nanos', 0)/1000000000)
                    break
            for item in machine_type_recommendations:
                if item['stateInfo']['state'] == 'ACTIVE' and item['content']['overview']['resourceName'] == instance['name']:
                    instance['recommendedMachineType'] = item['content']['overview']['recommendedMachineType']['name']
                    if 'additionalImpact' in item:
                        instance['recommendedMachineTypeSavings'] = -(int(item['additionalImpact'][0]['costProjection']['cost'].get('units', 0)) + item['additionalImpact'][0]['costProjection']['cost'].get('nanos', 0)/1000000000)
                    else:
                        instance['recommendedMachineTypeSavings'] = -(int(item['primaryImpact']['costProjection']['cost'].get('units', 0)) + item['primaryImpact']['costProjection']['cost'].get('nanos', 0)/1000000000)

            if instance.get('recommendedMachineType') != None and instance.get('recommendedMachineTypeSavings') != None:
                instance["recommendedMachineSeries"] = instance['recommendedMachineType'].split('-')[0]
                if instance["recommendedMachineSeries"] == 'custom':
                    instance["recommendedMachineSeries"] = 'n1'
                instance["recommendedMachineSeries"] = instance["recommendedMachineSeries"]
                if instance['recommendedMachineSeries'] == 'custom' or instance['recommendedMachineType'] in n1_shared_core_machines:
                    instance["recommendedMachineSeries"] = 'n1'
                if instance['recommendedMachineType'] in gce_machine_types_dict:
                    instance['recommendedGuestCpus'] = gce_machine_types_dict[instance['recommendedMachineType']]['guestCpus']
                    instance['recommendedMemoryGb'] = gce_machine_types_dict[instance['recommendedMachineType']]['memoryMb']/1024
                elif instance['recommendedMachineType'].find('-ext') != -1:
                    instance['recommendedGuestCpus'] = int(instance['recommendedMachineType'].split('-')[-3])
                    instance['recommendedMemoryGb'] = int(instance['recommendedMachineType'].split('-')[-2])/1024
                    for series in gce_machine_series_list:
                        if series['series'] == instance['recommendedMachineSeries'] and series['type'] != 'SHARED_CORE':
                            instance['recommendedExtendedMemoryGb'] = instance['recommendedMemoryGb'] - instance['recommendedGuestCpus'] * series['maxMemoryPerCpu']
                elif instance['recommendedMachineType'].split('-')[0] == 'e2' and instance['recommendedMachineType'].split('-')[2] == 'medium':
                    instance['recommendedGuestCpus'] = 1
                    instance['recommendedMemoryGb'] = int(instance['recommendedMachineType'].split('-')[-1])/1024
                else:
                    instance['recommendedGuestCpus'] = instance['recommendedMachineType'].split('-')[-2]
                    instance['recommendedMemoryGb'] = int(instance['recommendedMachineType'].split('-')[-1])/1024
            else:
                instance["recommendedMachineSeries"] = instance['machineSeries']
                if instance["recommendedMachineSeries"] == 'custom':
                    instance["recommendedMachineSeries"] = 'n1'
                instance['recommendedGuestCpus'] = instance['guestCpus']
                instance['recommendedMemoryGb'] = instance['memoryGb']
                instance['recommendedExtendedMemoryGb'] = instance.get('extendedMemoryGb', 0)

            if instance.get('recommendedExtendedMemoryGb') == None:
                instance['recommendedExtendedMemoryGb'] = 0
            if instance.get('extendedMemoryGb') == None:
                instance['extendedMemoryGb'] = 0

            instance['cudGuestCpus'] = instance['recommendedGuestCpus']
            instance['cudMemoryGb'] = instance['recommendedMemoryGb'] - instance['recommendedExtendedMemoryGb']

            if 'goog-gke-node' in instance.get('labels', {}):
                if 'cpuMetrics' in instance:
                    # if instance['cpuMetrics']['uptime']/60 < 480 :
                    #   instance['recommendedGuestCpus'] = 0
                    #   instance['recommendedMemoryMb'] = 0
                    #   instance['recommendedMemoryGb'] = 0
                    pass
                else:
                    instance['cudGuestCpus'] = 0
                    instance['cudMemoryGb'] = 0

            if instance.get('recommendedMachineType') in n1_shared_core_machines or 'deletionSavings' in instance:
                instance['cudGuestCpus'] = 0
                instance['cudMemoryGb'] = 0
    except:
        print(traceback.format_exc())
        sys.stdout.flush()
    
    try:
        for disk in gce_disks:
            for item in idle_disk_recommendations:
                if item['stateInfo']['state'] == 'ACTIVE':
                    if disk['name'] == item['content']['overview']['resourceName']:
                        disk['isIdle'] = True
                        disk['deletionSavings'] = -(int(item['primaryImpact']['costProjection']['cost'].get(
                            'units', 0)) + item['primaryImpact']['costProjection']['cost'].get('nanos', 0)/1000000000)
                        break
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        for image in gce_disk_images:
            for item in idle_image_recommendations:
                if item['stateInfo']['state'] == 'ACTIVE':
                    if image['name'] == item['content']['overview']['resourceName']:
                        image['isIdle'] = True
                        image['deletionSavings'] = -(int(item['primaryImpact']['costProjection']['cost'].get(
                            'units', 0)) + item['primaryImpact']['costProjection']['cost'].get('nanos', 0)/1000000000)
                        break
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        for ip in gce_addresses:
            for item in idle_ip_recommendations:
                if item['stateInfo']['state'] == 'ACTIVE':
                    if item['content']['overview']['resourceName'] == ip['name']:
                        ip['isIdle'] = True
                        ip['deletionSavings'] = -(int(item['primaryImpact']['costProjection']['cost'].get(
                            'units', 0)) + item['primaryImpact']['costProjection']['cost'].get('nanos', 0)/1000000000)
                        break
    except:
        print(traceback.format_exc())
        sys.stdout.flush()
    
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
    
    try:
        # retrieve and process cpu and memory metrics of instances
        target_project = project_id
        end = datetime.datetime.utcnow()
        start = now - datetime.timedelta(days=monitor_period)
        end_time = timestamp.Timestamp(seconds=int(end.timestamp()))
        start_time = timestamp.Timestamp(seconds=int(start.timestamp()))
        interval = monitoring_v3.types.TimeInterval(
            end_time=end_time, start_time=start_time)
        aggregation = monitoring_v3.types.Aggregation(
            alignment_period=duration.Duration(seconds=300),
            per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            cross_series_reducer=monitoring_v3.Aggregation.Reducer.REDUCE_NONE,
            group_by_fields = ['resource.labels."project_id"', 'resource.labels."database_id"', 'resource.labels."region"', 'metadata.labels."name"']
        )

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
    except:
        print(traceback.format_exc())
        sys.stdout.flush()
    
    try:
        temp_savings = 0
        for instance in gce_instances:
            if 'recommendedMachineType' in instance and 'deletionSavings' not in instance:
                temp_dict = {
                    'Project ID': f'{project_id}',
                    'Entity Type': 'Instance Machine Type Recommendation',
                    'Entity': instance['name'],
                    'Parent': f'{project_id}',
                    'Region': 'NA',
                    'Zone': instance['zone'].split('/')[-1],
                    'Configuration 01': f"Current Machine Type: {instance['machineType'].split('/')[-1]}",
                    'Configuration 02': f"Recommended Machine Type: {instance['recommendedMachineType']}",
                    'Configuration 03': 'NA',
                    'Pricing': '',
                    'Current Cost': '',
                    'New Cost': '',
                    'Savings 1': instance['recommendedMachineTypeSavings'],
                    'Savings 2': 'NA',
                    'Required Action': ''
                }
                table_export.append(temp_dict)
                for cud in new_cud:
                    if cud['machineType'].split('_')[-1].lower() == instance['machineSeries'] and instance['zone'].split('/')[-1].find(cud['region']) > -1:
                        if cud['resourceType'] == 'MEMORY':
                            cud['BREAK_EVEN_POINT_RESOURCE_VALUE'] = cud['BREAK_EVEN_POINT_RESOURCE_VALUE'] - (instance['memoryGb'] - instance['extendedMemoryGb'])
                        if cud['resourceType'] == 'VCPU':
                            cud['BREAK_EVEN_POINT_RESOURCE_VALUE'] = cud['BREAK_EVEN_POINT_RESOURCE_VALUE'] - int(instance['guestCpus'])
                    if cud['machineType'].split('_')[-1].lower() == instance['recommendedMachineSeries'] and instance['zone'].split('/')[-1].find(cud['region']) > -1:
                        if cud['resourceType'] == 'MEMORY':
                            cud['BREAK_EVEN_POINT_RESOURCE_VALUE'] = cud['BREAK_EVEN_POINT_RESOURCE_VALUE'] + (instance['recommendedMemoryGb'] - instance['recommendedExtendedMemoryGb'])
                        if cud['resourceType'] == 'VCPU':
                            cud['BREAK_EVEN_POINT_RESOURCE_VALUE'] = cud['BREAK_EVEN_POINT_RESOURCE_VALUE'] + int(instance['recommendedGuestCpus'])
                temp_savings += temp_dict['Savings 1']
                total_savings += temp_dict['Savings 1']
        if temp_savings > 0:
            temp_dict = {
                'Project ID': project_id,
                'Component': 'Instance Machine Type Recommendation Savings',
                'Total Savings': temp_savings
            }
            table_export_2.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()
    
    try:
        temp_savings = 0
        for instance in cloudsql_instances:
            if instance['settings'].get('tier') not in machine_type_exclusion:
                for item in sql_instance_cpu_utilization:
                    if item['database'] == instance['name']:
                        if item['uptime'] > 710:
                            temp_dict = {
                                'Project ID': f'{project_id}',
                                'Entity Type': 'Cloud SQL CUD',
                                'Entity': instance['name'],
                                'Parent': f'{project_id}',
                                'Region': instance['region'],
                                'Zone': instance['gceZone'],
                                'Configuration 01': f"Current Machine Tier: {instance['settings'].get('tier')}",
                                'Configuration 02': 'NA',
                                'Configuration 03': 'NA',
                                'Pricing': '',
                                'Current Cost': 0,
                                'New Cost': 0,
                                'Savings 1': '',
                                'Savings 2': '',
                                'Required Action': ''
                            }
                            temp_cost = 0
                            if instance['settings'].get('availabilityType', '') == 'REGIONAL':
                                for price in cloud_sql_pricing_list:
                                    if price['Region'] == instance['region'] and price['Component'] == 'HA vCPUs':
                                        temp_dict['Current Cost'] += price['Price']*instance['cpu']
                                        temp_dict['New Cost'] += price['_3_Year_Commitment']*instance['cpu']
                                        temp_cost += price['_1_Year_Commitment']*instance['cpu']
                                    if price['Region'] == instance['region'] and price['Component'] == 'HA Memory':
                                        temp_dict['Current Cost'] += price['Price']*instance['memoryGb']
                                        temp_dict['New Cost'] += price['_3_Year_Commitment']*instance['memoryGb']
                                        temp_cost += price['_1_Year_Commitment']*instance['memoryGb']
                            else:
                                for price in cloud_sql_pricing_list:
                                    if price['Region'] == instance['region'] and price['Component'] == 'vCPUs':
                                        temp_dict['Current Cost'] += price['Price']*instance['cpu']
                                        temp_dict['New Cost'] += price['_3_Year_Commitment']*instance['cpu']
                                        temp_cost += price['_1_Year_Commitment']*instance['cpu']
                                    if price['Region'] == instance['region'] and price['Component'] == 'Memory':
                                        temp_dict['Current Cost'] += price['Price']*instance['memoryGb']
                                        temp_dict['New Cost'] += price['_3_Year_Commitment']*instance['memoryGb']
                                        temp_cost += price['_1_Year_Commitment']*instance['memoryGb']
                            temp_dict['Savings 1'] = temp_dict['Current Cost'] - temp_dict['New Cost']
                            temp_dict['Savings 2'] = temp_dict['Current Cost'] - temp_cost
                            temp_savings += temp_dict['Savings 1']
                            total_savings += temp_dict['Savings 1']
                            table_export.append(temp_dict)
        if temp_savings > 0:
            temp_dict = {
                'Project ID': project_id,
                'Component': 'Cloud SQL CUD Savings',
                'Total Savings': temp_savings
            }
            table_export_2.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()
    
    try:
        temp_savings = 0
        for instance in gce_instances:
            if 'isIdle' in instance and instance['isIdle']:
                temp_dict = {
                    'Project ID': f'{project_id}',
                    'Entity Type': 'Idle Instance',
                    'Entity': instance['name'],
                    'Parent': f'{project_id}',
                    'Region': 'NA',
                    'Zone': instance['zone'].split('/')[-1],
                    'Configuration 01': f"Current Machine Type: {instance['machineType'].split('/')[-1]}",
                    'Configuration 02': "",
                    'Configuration 03': 'NA',
                    'Pricing': '',
                    'Current Cost': '',
                    'New Cost': '',
                    'Savings 1': instance['deletionSavings'],
                    'Savings 2': 'NA',
                    'Required Action': ''
                }
                table_export.append(temp_dict)
                for cud in new_cud:
                    if cud['machineType'].split('_')[-1].lower() == instance['machineSeries'] and instance['zone'].split('/')[-1].find(cud['region']) > -1:
                        if cud['resourceType'] == 'MEMORY':
                            cud['BREAK_EVEN_POINT_RESOURCE_VALUE'] = cud['BREAK_EVEN_POINT_RESOURCE_VALUE'] - (instance['memoryGb'] - instance['extendedMemoryGb'])
                        if cud['resourceType'] == 'VCPU':
                            cud['BREAK_EVEN_POINT_RESOURCE_VALUE'] = cud['BREAK_EVEN_POINT_RESOURCE_VALUE'] - int(instance['guestCpus'])
                temp_savings += temp_dict['Savings 1']
                total_savings += temp_dict['Savings 1']
        if temp_savings > 0:
            temp_dict = {
                'Project ID': project_id,
                'Component': 'Idle Instance Shutdown Savings',
                'Total Savings': temp_savings
            }
            table_export_2.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        temp_savings = 0
        for disk in gce_disks:
            if 'isIdle' in disk and disk['isIdle']:
                temp_dict = {
                    'Project ID': f'{project_id}',
                    'Entity Type': 'Idle Disk',
                    'Entity': disk['name'],
                    'Parent': f'{project_id}',
                    'Region': 'NA',
                    'Zone': 'NA',
                    'Configuration 01': f"{disk['type'].split('/')[-1]}: {disk['sizeGb']} GB",
                    'Configuration 02': "",
                    'Configuration 03': 'NA',
                    'Pricing': '',
                    'Current Cost': '',
                    'New Cost': '',
                    'Savings 1': disk['deletionSavings'],
                    'Savings 2': 'NA',
                    'Required Action': ''
                }
                if 'zone' in disk:
                    temp_dict['zone'] = disk['zone'].split('/')[-1]
                else:
                    temp_dict['region'] = disk['region'].split('/')[-1]
                table_export.append(temp_dict)
                temp_savings += temp_dict['Savings 1']
                total_savings += temp_dict['Savings 1']
        if temp_savings > 0:
            temp_dict = {
                'Project ID': project_id,
                'Component': 'Idle Disk Deletion Savings',
                'Total Savings': temp_savings
            }
            table_export_2.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        temp_savings = 0
        for address in gce_addresses:
            if 'isIdle' in address and address['isIdle']:
                temp_dict = {
                    'Project ID': f'{project_id}',
                    'Entity Type': 'Idle IP',
                    'Entity': address['name'],
                    'Parent': f'{project_id}',
                    'Region': 'NA',
                    'Zone': 'NA',
                    'Configuration 01': f"{address['address']}",
                    'Configuration 02': "",
                    'Configuration 03': 'NA',
                    'Pricing': '',
                    'Current Cost': '',
                    'New Cost': '',
                    'Savings 1': address['deletionSavings'],
                    'Savings 2': 'NA',
                    'Required Action': ''
                }
                table_export.append(temp_dict)
                temp_savings += temp_dict['Savings 1']
                total_savings += temp_dict['Savings 1']
        if temp_savings > 0:
            temp_dict = {
                'Project ID': project_id,
                'Component': 'Idle IP Deletion Savings',
                'Total Savings': temp_savings
            }
            table_export_2.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        temp_savings = 0
        for image in gce_disk_images:
            if 'isIdle' in image and image['isIdle']:
                temp_dict = {
                    'Project ID': f'{project_id}',
                    'Entity Type': 'Idle Disk Image',
                    'Entity': image['name'],
                    'Parent': f'{project_id}',
                    'Region': 'NA',
                    'Zone': 'NA',
                    'Configuration 01': "",
                    'Configuration 02': f"Archive Size: {round(int(image['archiveSizeBytes'])/(1024*1024), 2)} MB",
                    'Configuration 03': 'NA',
                    'Pricing': '',
                    'Current Cost': '',
                    'New Cost': '',
                    'Savings 1': image['deletionSavings'],
                    'Savings 2': 'NA',
                    'Required Action': ''
                }
                if 'sourceDisk' in image:
                    temp_dict['Configuration 01'] = f"Source Disk: {image['sourceDisk'].split('/')[-1]}"
                if 'sourceSnapshot' in image:
                    temp_dict['Configuration 01'] = f"Source Snapshot: {image['sourceSnapshot'].split('/')[-1]}"
                if 'sourceImage' in image:
                    temp_dict['Configuration 01'] = f"Source Image: {image['sourceImage'].split('/')[-1]}"
                table_export.append(temp_dict)
                temp_savings += temp_dict['Savings 1']
                total_savings += temp_dict['Savings 1']
        if temp_savings > 0:
            temp_dict = {
                'Project ID': project_id,
                'Component': 'Idle Disk Image Deletion Savings',
                'Total Savings': temp_savings
            }
            table_export_2.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()
    
    try:
        temp_savings = 0
        for disk in gce_disks:
            if 'isIdle' not in disk or not disk['isIdle']:
                if disk['type'].split('/')[-1] == 'pd-ssd':
                    temp_dict = {
                        'Project ID': f'{project_id}',
                        'Entity Type': 'SSD-BPD Migration',
                        'Entity': disk['name'],
                        'Parent': f'{project_id}',
                        'Region': 'NA',
                        'Zone': 'NA',
                        'Configuration 01': f"{disk['type'].split('/')[-1]}: {disk['sizeGb']} GB",
                        'Configuration 02': "",
                        'Configuration 03': 'NA',
                        'Pricing': '',
                        'Current Cost': '',
                        'New Cost': '',
                        'Savings 1': float(disk['sizeGb']) * 0.07,
                        'Savings 2': 'NA',
                        'Required Action': ''
                    }
                    if 'zone' in disk:
                        temp_dict['zone'] = disk['zone'].split('/')[-1]
                    else:
                        temp_dict['region'] = disk['region'].split('/')[-1]
                    table_export.append(temp_dict)
                    temp_savings += temp_dict['Savings 1']
                    total_savings += temp_dict['Savings 1']
        if temp_savings > 0:
            temp_dict = {
                'Project ID': project_id,
                'Component': 'SSD-BPD Migration Savings',
                'Total Savings': temp_savings
            }
            table_export_2.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        temp_savings = 0
        for cud in gce_cud_recommendations:
            for item in new_cud:
                if item['recommendation'] == cud['recommendation']:
                    temp_dict = {
                        'Project ID': f'{project_id}',
                        'Entity Type': 'Compute Engine CUD',
                        'Entity': f"{cud['machineType']}_{cud['resourceType']}",
                        'Parent': f'{project_id}',
                        'Region': cud['region'],
                        'Zone': 'NA',
                        'Configuration 01': f"Old Resource Value: {cud['BREAK_EVEN_POINT_RESOURCE_VALUE']}",
                        'Configuration 02': f"New Resource Value: {item['BREAK_EVEN_POINT_RESOURCE_VALUE']}",
                        'Configuration 03': 'Commmit Period: 3 Years',
                        'Pricing': '',
                        'Current Cost': '',
                        'New Cost': '',
                        'Savings 1': cud['THIRTY_SIX_MONTH_BREAK_EVEN_POINT']*item['BREAK_EVEN_POINT_RESOURCE_VALUE']/cud['BREAK_EVEN_POINT_RESOURCE_VALUE'],
                        'Savings 2': cud['TWELVE_MONTH_BREAK_EVEN_POINT']*item['BREAK_EVEN_POINT_RESOURCE_VALUE']/cud['BREAK_EVEN_POINT_RESOURCE_VALUE'],
                        'Required Action': ''
                    }
                    temp_savings += temp_dict['Savings 1']
                    total_savings += temp_dict['Savings 1']
                    table_export.append(temp_dict)
                    break
        if temp_savings > 0:
            temp_dict = {
                'Project ID': project_id,
                'Component': 'Compute Engine CUD Savings',
                'Total Savings': temp_savings
            }
            table_export_2.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()
    
    try:
        temp_dict = {
            'Project ID': project_id,
            'Component': 'Total Savings',
            'Total Savings': total_savings
        }
        table_export_2.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    if len(table_export) > 0:
        try:
            gsheet.add_worksheet(title='COST_OPTIMIZATION', rows=0, cols=0)
        except:
            print(traceback.format_exc())
            sys.stdout.flush()
        ws = gsheet.worksheet("COST_OPTIMIZATION")
        df = pd.DataFrame(table_export)
        df = df[['Project ID', 'Entity Type', 'Entity', 'Parent', 'Region', 'Zone', 'Configuration 01', 'Configuration 02', 
                'Configuration 03', 'Pricing', 'Current Cost', 'New Cost', 'Savings 1', 'Savings 2', 'Required Action']]
        set_with_dataframe(ws, df)

    if len(table_export_2) > 0:
        try:
            gsheet.add_worksheet(title='COST_OPTIMIZATION_SUMMARY', rows=0, cols=0)
        except:
            print(traceback.format_exc())
            sys.stdout.flush()
        ws = gsheet.worksheet("COST_OPTIMIZATION_SUMMARY")
        df = pd.DataFrame(table_export_2)
        df = df[['Project ID', 'Component', 'Total Savings']]
        set_with_dataframe(ws, df)