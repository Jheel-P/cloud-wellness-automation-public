# import modules
import sys
import datetime
import gspread
import requests
import pandas as pd
from dateutil import parser
from gspread_dataframe import set_with_dataframe
from google.cloud import monitoring_v3
import traceback
import variables


def compute_network():
    # set module variables
    project_id = variables.project_id
    gsheet_url = variables.sheet_url
    monitor_period = 30             # days
    now = datetime.datetime.utcnow()

    # build compute client
    from googleapiclient import discovery
    monitoring_client = monitoring_v3.MetricServiceClient(credentials=variables.credentials)
    compute_service = discovery.build('compute', 'v1', credentials=variables.credentials)
    gspread_client = gspread.authorize(variables.credentials)
    gsheet = gspread_client.open_by_url(gsheet_url)

    gce_networks = []
    gce_subnetworks = []
    gce_firewall_rules = []
    gce_instances = []
    gce_backend_services = []
    gce_forwarding_rules = []
    gce_backends_health_status = []
    gce_ssl_certificates = []
    gce_routers = []

    try:
        # retrieve gce networks and subnetworks
        request = compute_service.networks().list(project=project_id)
        while request is not None:
            response = request.execute()
            for network in response['items']:
                gce_networks.append(network)
            request = compute_service.networks().list_next(
                previous_request=request, previous_response=response)

        request = compute_service.subnetworks().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for name, subnetworks_scoped_list in response['items'].items():
                if 'subnetworks' in subnetworks_scoped_list:
                    gce_subnetworks += subnetworks_scoped_list['subnetworks']
            request = compute_service.subnetworks().aggregatedList_next(
                previous_request=request, previous_response=response)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()
    
    table_export = []
    try:
        for network in gce_networks :
            temp_dict = {
                "Project ID": project_id,
                "Entity Type": "VPC Network",
                "Entity": network['name'],
                "Parent": f"projects/{project_id}",
                "Region": 'NA',
                "Auto Create Subnet": network['autoCreateSubnetworks'],
                "Peerings": "",
                "Private Google Access": 'NA',
                "Logging Enabled": 'NA',
                "Required Action": ""
            }
            text = ''''''
            for peering in network.get('peerings', []) :
                text = f"{text}- {peering['name']} : {peering['state']}\n"
            temp_dict['Peerings'] = text
            table_export.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        for subnetwork in gce_subnetworks :
            temp_dict = {
                "Project ID": project_id,
                "Entity Type": "VPC Subnet",
                "Entity": subnetwork['name'],
                "Parent": f"{subnetwork['network'].split('/')[-1]}",
                "Region": f"{subnetwork['region'].split('/')[-1]}",
                "Auto Create Subnet": 'NA',
                "Peerings": "NA",
                "Private Google Access": subnetwork['privateIpGoogleAccess'],
                "Logging Enabled": '',
                "Required Action": ""
            }
            if 'logConfig' in subnetwork:
                temp_dict['Logging Enabled'] = subnetwork['logConfig'].get('enable', False)
            else :
                temp_dict['Logging Enabled'] = False
            table_export.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    if len(table_export) > 0:
        try:
            gsheet.add_worksheet(title='COMPUTE_NETWORKS', rows=0, cols=0)
        except:
            print(traceback.format_exc())
            sys.stdout.flush()
        ws = gsheet.worksheet("COMPUTE_NETWORKS")
        df = pd.DataFrame(table_export)
        df = df[['Project ID', 'Entity Type', 'Entity', 'Parent', 'Region', 'Auto Create Subnet', 'Peerings', 
                'Private Google Access', 'Logging Enabled', 'Required Action']]
        set_with_dataframe(ws, df)

    try:
        # retrieve gce firewall rules
        request = compute_service.firewalls().list(project=project_id)
        while request is not None:
            response = request.execute()
            for firewall in response['items']:
                gce_firewall_rules.append(firewall)
            request = compute_service.firewalls().list_next(
                previous_request=request, previous_response=response)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()
    
    try:
        request = compute_service.instances().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for name, instances_scoped_list in response['items'].items():
                gce_instances += instances_scoped_list.get('instances', [])
            request = compute_service.instances().aggregatedList_next(
                previous_request=request, previous_response=response)

        # get list of effective firewall rules
        for instance in gce_instances:
            for nic in instance.get('networkInterfaces', []):
                url = f"https://compute.googleapis.com/compute/v1/projects/{project_id}/zones/{instance['zone'].split('/')[-1]}/instances/{instance['name']}/getEffectiveFirewalls"
                params = {
                    'networkInterface': nic['name']
                }
                response = requests.get(url, headers=variables.oauth_header, params=params)
                nic['effectiveFirewalls'] = response.json()
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    table_export = []
    try:
        for rule in gce_firewall_rules:
            temp_dict = {
                "Project ID": project_id,
                "Entity Type": "Firewall Rule",
                "Entity": rule['name'],
                "Parent": f"{rule['network'].split('/')[-1]}",
                "Direction": rule['direction'],
                "Disabled": rule['disabled'],
                "Allowed": "",
                "Source Ranges": "NA",
                "Logging Enabled": rule.get('logConfig', False).get('enable', False),
                "Critical Access": 'None',
                "Applicable to Instances": '',
                "Required Action": ''
            }
            text = ''''''
            for instance in gce_instances :
                found = False
                for interface in instance.get('networkInterfaces', []):
                    for item in interface.get('effectiveFirewalls', {}).get('firewalls', []):
                        if item['name'] == rule['name'] :
                            text = f"{text}- {instance['name']}\n"
                            found = True
                            break
                    if found:
                        break
            temp_dict['Applicable to Instances'] = text
            text = ''''''
            for range in rule.get('sourceRanges', []):
                text = f"{text}- {range}\n"
            text = text[:-1]
            temp_dict['Source Ranges'] = text
            critical_ports = ['22', '3389']
            text = ''''''
            text_1 = ''''''
            for allowed in rule.get('allowed', []):
                if allowed['IPProtocol'] == 'all':
                    text = f"{text}- all\n"
                    if '0.0.0.0/0' in rule.get('sourceRanges', []):
                        text_1 = f"{text_1}- all\n"
                elif 'ports' in allowed:
                    temp_str = ''
                    temp_str_1 = ''
                    for port in allowed['ports']:
                        temp_str = f"{temp_str}, {port}"
                        if '0.0.0.0/0' in rule.get('sourceRanges', []) and allowed['IPProtocol'] == 'tcp':
                            if len(port.split('-')) > 1:
                                for critical_port in critical_ports:
                                    if int(port.split('-')[0]) <= int(critical_port) and int(port.split('-')[1]) >= int(critical_port):
                                        temp_str_1 = f"{temp_str_1}, {port}"
                            else:
                                if port in critical_ports:
                                    temp_str_1 = f"{temp_str_1}, {port}"
                    if temp_str_1 != '':
                        temp_str_1 = temp_str_1[1:]
                        temp_str_1 = f"{allowed['IPProtocol']}:{temp_str_1}"
                        text_1 = f"{text_1}- {temp_str_1}\n"
                    temp_str = temp_str[1:]
                    temp_str = f"{allowed['IPProtocol']}:{temp_str}"
                    text = f"{text}- {temp_str}\n"
                elif allowed['IPProtocol'] in ['tcp', 'udp']:
                    temp_str = f"{allowed['IPProtocol']}: all"
                    text = f"{text}- {temp_str}\n"
                    if '0.0.0.0/0' in rule.get('sourceRanges', []):
                        text_1 = f"{text_1}- {temp_str}\n"
                else :
                    temp_str = f"{allowed['IPProtocol']}"
                    text = f"{text}- {temp_str}\n"
                    if '0.0.0.0/0' in rule.get('sourceRanges', []):
                        text_1 = f"{text_1}- {temp_str}\n"
            text = text[:-1]
            text_1 = text_1[:-1]
            temp_dict['Allowed'] = text
            temp_dict['Critical Access'] = text_1
            table_export.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    if len(table_export) > 0:
        try:
            gsheet.add_worksheet(title='COMPUTE_FIREWALL_RULES', rows=0, cols=0)
        except:
            print(traceback.format_exc())
            sys.stdout.flush()
        ws = gsheet.worksheet("COMPUTE_FIREWALL_RULES")
        df = pd.DataFrame(table_export)
        df = df[['Project ID', 'Entity Type', 'Entity', 'Parent', 'Direction', 'Disabled', 'Allowed', 
                'Source Ranges', 'Logging Enabled', 'Critical Access', 'Applicable to Instances', 'Required Action']]
        set_with_dataframe(ws, df)

    try:
        # retrieve gce lb components
        request = compute_service.backendServices().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for name, backend_services_scoped_list in response['items'].items():
                if 'backendServices' in backend_services_scoped_list:
                    gce_backend_services += backend_services_scoped_list['backendServices']
            request = compute_service.backendServices().aggregatedList_next(
                previous_request=request, previous_response=response)
        request = compute_service.forwardingRules().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for name, forwarding_rules_scoped_list in response['items'].items():
                if 'forwardingRules' in forwarding_rules_scoped_list:
                    gce_forwarding_rules += forwarding_rules_scoped_list['forwardingRules']
            request = compute_service.forwardingRules().aggregatedList_next(
                previous_request=request, previous_response=response)
        for backend_service in gce_backend_services:
            for backend in backend_service.get('backends', []):
                if backend['group'].split('/')[-2] != None:
                    url = f"{backend_service['selfLink']}/getHealth"
                    body = {
                        "group": backend['group']
                    }
                    response = requests.post(url, headers=variables.oauth_header, json=body)
                    temp_dict = {
                        'healthStatus': response.json().get('healthStatus', []),
                        'backendService': backend_service['name'],
                        'group': backend['group']
                    }
                    gce_backends_health_status.append(temp_dict)
                    # backend['healthStatus'] = response.json().get('healthStatus')
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        request = compute_service.sslCertificates().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for name, ssl_certificates_scoped_list in response['items'].items():
                if 'sslCertificates' in ssl_certificates_scoped_list:
                    gce_ssl_certificates += ssl_certificates_scoped_list['sslCertificates']
            request = compute_service.sslCertificates().aggregatedList_next(
                previous_request=request, previous_response=response)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    table_export = []
    try:
        for item in gce_backends_health_status:
            if len(item['healthStatus']) > 0:
                temp_dict = {
                    "Project ID": project_id,
                    "Entity Type": "Backend Group",
                    "Entity": f"{item['group'].split('/')[-1]}",
                    "Parent": f"{item['backendService']}",
                    "Health Status": 'NA',
                    "Unhealthy": False,
                    "Certificate Expire Days": 'NA',
                    "Certificate Expired": 'NA',
                    "Required Action": ''
                }
                text = ''''''
                for status in item['healthStatus']:
                    text = f"{text}- {status['instance'].split('/')[-1]}: {status['healthState']}\n"
                    if status['healthState'] != 'HEALTHY':
                        temp_dict['Unhealthy'] = True
                text = text[:-1]
                temp_dict['Health Status'] = text
                table_export.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    try:
        for item in gce_ssl_certificates:
            temp_dict = {
                "Project ID": project_id,
                "Entity Type": "SSL Certificate",
                "Entity": f"{item['name']}",
                "Parent": f"projects/{project_id}",
                "Health Status": 'NA',
                "Unhealthy": 'NA',
                "Certificate Expire Days": (parser.parse(item['expireTime']).replace(tzinfo=None)-datetime.datetime.now()).days,
                "Certificate Expired": 'NA',
                "Required Action": ''
            }
            if (parser.parse(item['expireTime']).replace(tzinfo=None)-datetime.datetime.now()).days <= 0:
                temp_dict['Certificate Expired'] = True
            else:
                temp_dict['Certificate Expired'] = False
            table_export.append(temp_dict)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()

    if len(table_export) > 0:
        try:
            gsheet.add_worksheet(title='COMPUTE_LOAD_BALANCERS', rows=0, cols=0)
        except:
            print(traceback.format_exc())
            sys.stdout.flush()
        ws = gsheet.worksheet("COMPUTE_LOAD_BALANCERS")
        df = pd.DataFrame(table_export)
        df = df[['Project ID', 'Entity Type', 'Entity', 'Parent', 'Health Status', 'Unhealthy', 'Certificate Expire Days', 
                'Certificate Expired', 'Required Action']]
        set_with_dataframe(ws, df)

    try:
        # retrieve gce routers
        request = compute_service.routers().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for name, routers_scoped_list in response['items'].items():
                if 'routers' in routers_scoped_list:
                    gce_routers += routers_scoped_list['routers']
            request = compute_service.routers().aggregatedList_next(
                previous_request=request, previous_response=response)
    except:
        print(traceback.format_exc())
        sys.stdout.flush()