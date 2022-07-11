import sys
import variables
import base64
import json
from iam import iam
from cost_optimization import cost_optimization
from compute_network import compute_network
from compute_engine import compute_engine
from database import database
from kubernetes_engine import kubernetes_engine
from google.cloud import datastore
from flask import Flask
from flask import request, Response


datastore_client = datastore.Client(project=variables.bq_job_project_id)

app = Flask('post method')

@app.route("/assess_project", methods=["POST"])
def MAIN():
    json_body = request.json
    payload = json.loads(base64.b64decode(json_body['message']['data']).decode())
    print(payload)
    sys.stdout.flush()
    variables.project_id = payload['project_id']
    variables.sheet_url = payload['sheet_url']
    
    name = f"{variables.project_id}_{payload.get('assessment')}"
    task_key = datastore_client.key(variables.kind, name)
    task = datastore_client.get(task_key)
    if task is not None:
        if task.get('status') == 'INPROGRESS':
            print(f"Assessment in progress for project {variables.project_id}")
            sys.stdout.flush()
            return f"Assessment in progress for project {variables.project_id}"
    
    name = name = f"{variables.project_id}_{payload.get('assessment')}"
    task_key = datastore_client.key(variables.kind, name)
    task = datastore.Entity(key=task_key)
    task.update({
        'project_id': variables.project_id,
        'sheet_url': variables.sheet_url,
        'assessment': payload.get('assessment'),
        'status': 'INPROGRESS'
    })
    datastore_client.put(task)
    print(f"Assessment started for project {variables.project_id}")
    print(f"Saved task {task.key.name}")
    sys.stdout.flush()

    if payload.get('assessment') == 'cost_optimization':
        cost_optimization()
    if payload.get('assessment') == 'compute_engine':
        compute_engine()
    if payload.get('assessment') == 'compute_network':
        compute_network()
    if payload.get('assessment') == 'iam':
        iam()
    if payload.get('assessment') == 'database':
        database()
    if payload.get('assessment') == 'kubernetes_engine':
        kubernetes_engine()

    return f"Successfully assessed project {variables.project_id}"

    # return "SUCCESS"


if __name__ == '__main__':
    app.run(host = '0.0.0.0', port = 8080)

