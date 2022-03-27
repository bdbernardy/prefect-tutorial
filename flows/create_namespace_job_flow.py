import time

from prefect import Flow
from prefect.storage import GitHub
from prefect.executors import LocalDaskExecutor


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
                        "image": "eu.gcr.io/busbud-integrations/k8-tests:latest",
                        "command": ["node",  "wait.js"]
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
    body=create_template("job3"), namespace="ua-prefect", kubernetes_api_key_secret=None)  # type: ignore


with Flow("parallel-flow") as flow:
    job1_task = job1()
    job2_task = job2()
    job3(upstream_tasks=[job1_task, job2_task])

# Storing flow in github
flow.storage = GitHub(
    repo="bdbernardy/prefect-tutorial",                           # name of repo
    path="flows/create_namespace_job_flow.py"                   # location of flow file in repo
    # access_token_secret="GITHUB_ACCESS_TOKEN"  # name of personal access token secret
)

flow.executor = LocalDaskExecutor()

# Register the flow under the "tutorial" project
flow.register(project_name="tutorial")
