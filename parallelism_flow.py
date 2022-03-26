from asyncio import wait_for
import os
import time

import prefect
from prefect import task, Flow, Parameter
from prefect.run_configs import UniversalRun
from prefect.storage import GitHub

from prefect.tasks.kubernetes import CreateNamespacedJob


def create_template(name: str):
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [{
                        "name": name,
                        "image": "eu.gcr.io/busbud-integrations/k8-tests:latest"
                        # "command": ["perl",  "-Mbignum=bpi", "-wle", "print bpi(2000)"]
                    }],
                    "restartPolicy": "Never"
                },
            },
            "backoffLimit": 4
        }
    }


job1 = CreateNamespacedJob(
    name="job1",
    body=create_template("job1"), namespace="ua-prefect", kubernetes_api_key_secret=None)  # type: ignore

job2 = CreateNamespacedJob(
    name="job2",
    body=create_template("job2"), namespace="ua-prefect", kubernetes_api_key_secret=None)  # type: ignore

job3 = CreateNamespacedJob(
    name="job3",
    body=create_template("job2"), namespace="ua-prefect", kubernetes_api_key_secret=None)  # type: ignore

with Flow("parallel-flow") as flow:
    job1()
    job2()
    job3(upstream_tasks=[job1, job2])

# Storing flow in github
flow.storage = GitHub(
    repo="bdbernardy/prefect-tutorial",                           # name of repo
    path="parallelism_flow.py"                   # location of flow file in repo
    # access_token_secret="GITHUB_ACCESS_TOKEN"  # name of personal access token secret
)

# Register the flow under the "tutorial" project
flow.register(project_name="tutorial")
