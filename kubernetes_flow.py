import os
import time

import prefect
from prefect import task, Flow, Parameter
from prefect.run_configs import UniversalRun
from prefect.storage import GitHub

from prefect.tasks.kubernetes import CreateNamespacedJob


@task
def say_hello(name):
    # Add a sleep to simulate some long-running task
    time.sleep(10)
    # Load the greeting to use from an environment variable
    greeting = os.environ.get("GREETING")
    logger = prefect.context.get("logger")
    logger.info(f"{greeting}, {name}!")


yaml_template = {
    "apiVersion": "batch/v1",
    "kind": "Job",
    "metadata": {
        "name": "pi"
    },
    "spec": {
        "template": {
            "spec": {
                "containers": [{
                    "name": "pi",
                    "image": "perl",
                    "command": ["perl",  "-Mbignum=bpi", "-wle", "print bpi(2000)"]
                }]
            },
            "restartPolicy": "Never"
        },
        "backoffLimit": 4
    }
}

say_hello_job = CreateNamespacedJob(
    body=yaml_template, namespace="ua-prefect", kubernetes_api_key_secret=None)  # type: ignore

with Flow("hello-flow") as flow:
    people = Parameter("people", default=["Arthur", "Ford", "Marvin"])
    say_hello.map(people)
    say_hello_job()

# Configure the `GREETING` environment variable for this flow
flow.run_config = UniversalRun(env={"GREETING": "Hello"})

# Storing flow in github
flow.storage = GitHub(
    repo="bdbernardy/prefect-tutorial",                           # name of repo
    path="kubernetes_flow.py"                   # location of flow file in repo
    # access_token_secret="GITHUB_ACCESS_TOKEN"  # name of personal access token secret
)

# Register the flow under the "tutorial" project
flow.register(project_name="tutorial")
