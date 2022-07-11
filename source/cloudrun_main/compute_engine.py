# import modules
import sys
import datetime
import gspread
import requests
import pandas as pd
from gspread_dataframe import set_with_dataframe
from google.cloud import monitoring_v3, bigquery
import traceback
import variables


def compute_engine():
    # set module variables
    project_id = variables.project_id
    bq_job_project_id = variables.bq_job_project_id
    bq_dataset_project_id = variables.bq_dataset_project_id
    gsheet_url = variables.sheet_url
    monitor_period = 30             # days
    now = datetime.datetime.utcnow()

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
    table_export = []

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
            
            if 'metadata' in instance:
                for item in instance['metadata'].get('items', []):
                    if item.get('key') == 'cluster-name':
                        instance['gkeCluster'] = item.get('value')
                    if item.get('key') == 'created-by':
                        instance['createdBy'] = item.get('value')
    except:
        pass

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
        for instance in gce_instances:
            temp_dict = {
                'Project ID': f'{project_id}',
                'Entity Type': 'Compute Instance',
                'Entity': instance['name'],
                'Parent': instance.get('createdBy', f'Project: {project_id}').split('/')[-1],
                'Zone': instance['zone'].split('/')[-1],
                'Machine Type': f"{instance['machineType'].split('/')[-1]}",
                'Machine Series': f"{instance['machineSeries']}",
                'CPU': float(instance['guestCpus']),
                'Memory (GB)': (instance['memoryGb'] - instance['extendedMemoryGb']),
                'Extended Memory (GB)': instance['extendedMemoryGb'],
                'Recommended Machine Type': instance.get('recommendedMachineType', ''),
                'Recommended Machine Series': instance.get('recommendedMachineSeries', ''),
                'Recommended CPU': float(instance.get('recommendedGuestCpus', '')),
                'Recommended Memory (GB)': instance.get('recommendedMemoryGb', '') - instance.get('recommendedExtendedMemoryGb', ''),
                'Recommended Extended Memory (GB)': instance.get('recommendedExtendedMemoryGb', ''),
                'Instance Idle': instance.get('isIdle', False),
                'Savings': instance.get('deletionSavings', instance.get('recommendedMachineTypeSavings', 0)),
                'Boot Disk': '',
                'Boot Disk Licenses': '',
                'OS Deprecated': '',
                'Additional Disks': '',
                'Service Account': '',
                'Access Scopes': 'Default/Custom',
                'Ops Agent': '',
                'State': instance.get('status'),
                'GKE Cluster': instance.get('gkeCluster', 'NA'),
                'Mean CPU Utilization': '',
                'Mean Memory Utilization': '',
                'Deletion Protection': instance.get('deletionProtection', False),
                'Required Action': '',
            }
            text = ''''''
            text_01 = ''''''
            for disk in instance.get('disks', []):
                if disk.get('boot', False):
                    for license in disk.get('licenses', []):
                        text_01 = f"{text_01}- {license.split('/')[-1]}\n"
                    for item in gce_disks:
                        if disk.get('source').split('/')[-1] == item.get('name'):
                            temp_dict['Boot Disk'] = f"{item.get('type').split('/')[-1]}: {disk.get('diskSizeGb')}"
                            break
                else:
                    for item in gce_disks:
                        if disk.get('source').split('/')[-1] == item.get('name'):
                            text = f"{text}- {item.get('type').split('/')[-1]}: {disk.get('diskSizeGb')}\n"
            if len(text) > 0:
                text = text[:-1]
            if len(text_01) > 0:
                text_01 = text_01[:-1]
            temp_dict['Additional Disks'] = text
            temp_dict['Boot Disk Licenses'] = text_01
            text = ''''''
            for account in instance.get('serviceAccounts', []):
                text = f"{text}- {account.get('email')}\n"
                if 'scopes' in account:
                    if 'https://www.googleapis.com/auth/cloud-platform' in account['scopes']:
                        temp_dict['Access Scopes'] = 'Full'
            if len(text) > 0:
                text = text[:-1]
            temp_dict['Service Account'] = text
            table_export.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    if len(table_export) > 0:
        try:
            gsheet.add_worksheet(title='COMPUTE_INSTANCES', rows=0, cols=0)
        except:
            print(traceback.format_exc())
            sys.stdout.flush()
        ws = gsheet.worksheet("COMPUTE_INSTANCES")
        df = pd.DataFrame(table_export)
        df = df[['Project ID', 'Entity Type', 'Entity', 'Parent', 'Zone', 'Machine Type', 'Machine Series', 'CPU', 
                'Memory (GB)', 'Extended Memory (GB)', 'Recommended Machine Type', 'Recommended Machine Series', 
                'Recommended CPU', 'Recommended Memory (GB)', 'Recommended Extended Memory (GB)', 'Instance Idle',
                'Savings', 'Boot Disk', 'Boot Disk Licenses', 'OS Deprecated', 'Additional Disks', 'Service Account',
                'Access Scopes', 'Ops Agent', 'State', 'GKE Cluster', 'Mean CPU Utilization', 'Mean Memory Utilization', 
                'Deletion Protection', 'Required Action']]
        set_with_dataframe(ws, df)