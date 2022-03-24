import os
import time

import prefect
from prefect import task, Flow, Parameter
from prefect.run_configs import UniversalRun


@task
def say_hello(name):
    # Add a sleep to simulate some long-running task
    time.sleep(10)
    # Load the greeting to use from an environment variable
    greeting = os.environ.get("GREETING")
    logger = prefect.context.get("logger")
    logger.info(f"{greeting}, {name}!")


with Flow("hello-flow") as flow:
    people = Parameter("people", default=["Arthur", "Ford", "Marvin"])
    say_hello.map(people)

# Configure the `GREETING` environment variable for this flow
flow.run_config = UniversalRun(env={"GREETING": "Hello"})

# Register the flow under the "tutorial" project
# flow.register(project_name="tutorial")
