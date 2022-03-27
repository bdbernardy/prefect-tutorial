import os
import time
from uuid import uuid1

import prefect
from prefect import task, Flow, Parameter
from prefect.run_configs import UniversalRun
from prefect.executors import LocalDaskExecutor
from prefect.storage import GitHub


@task
def generate_id():
    generated_id = uuid1()
    logger = prefect.context.get("logger")
    logger.info(f"Generated id {generated_id}.")
    time.sleep(5)
    return generated_id


@task
def before_hello():
    logger = prefect.context.get("logger")
    logger.info("Before hello")
    time.sleep(10)


@task
def during_hello():
    logger = prefect.context.get("logger")
    logger.info("During hello")
    time.sleep(3)


@task
def say_hello(name: str):
    time.sleep(10)
    # Load the greeting to use from an environment variable
    greeting = os.environ.get("GREETING")
    logger = prefect.context.get("logger")
    logger.info(f"{greeting}, {name}!")


@task
def after_hello(generated_id: str):
    logger = prefect.context.get("logger")
    logger.info(f"After hello. The generated id was {generated_id}")


with Flow("dependencies-flow") as flow:
    people = Parameter("people", default=["Abdul", "Gaston", "Marvin"])

    my_id = generate_id()
    before_hello_task = before_hello()

    say_hello_tasks = say_hello.map(people)
    say_hello_tasks.set_upstream(before_hello_task)

    during_hello_task = during_hello()
    during_hello_task.set_upstream(before_hello_task)

    after_hello_task = after_hello(my_id)
    after_hello_task.set_upstream(say_hello_tasks)
    after_hello_task.set_upstream(during_hello_task)

# Configure the `GREETING` environment variable for this flow
flow.run_config = UniversalRun(env={"GREETING": "Hello"})

flow.executor = LocalDaskExecutor()

flow.storage = GitHub(
    repo="bdbernardy/prefect-tutorial",                           # name of repo
    path="flows/dependencies_flow.py"                   # location of flow file in repo
    # access_token_secret="GITHUB_ACCESS_TOKEN"  # name of personal access token secret
)

# Register the flow under the "tutorial" project
flow.register(project_name="tutorial")
