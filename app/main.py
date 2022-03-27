import time
import sys
import logging
from uuid import uuid1
from datetime import datetime


def main():
    run_id = uuid1()

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    logging.info("Starting run %s.", run_id)

    for _ in range(6):
        now = datetime.now()
        logging.info("Processing run %s - the date is %s", run_id, now)
        time.sleep(10)

    logging.info("Done with run %s.", run_id)


if __name__ == '__main__':
    main()
