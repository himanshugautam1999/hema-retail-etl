import json
import os
import urllib.parse

import boto3

from common.logging_utils import get_logger

STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN", "")
EXPECTED_PREFIX = os.environ.get("LANDING_PREFIX", "landing/")

sfn = boto3.client("stepfunctions")


def handler(event, context):
    log = get_logger("trigger_lambda")

    records = event.get("Records", [])
    if not records:
        log.warning("Event contained no S3 records; nothing to do.")
        return {"started": 0}

    started = 0
    for record in records:
        bucket = record["s3"]["bucket"]["name"]
        # S3 event keys are URL-encoded (spaces become '+', etc) that's why we need to decode them
        raw_key = record["s3"]["object"]["key"]
        key = urllib.parse.unquote_plus(raw_key)

        run_log = get_logger("trigger_lambda", {"bucket": bucket, "key": key})

        if not key.startswith(EXPECTED_PREFIX):
            run_log.info("Key not under landing prefix; skipping.")
            continue
        if not key.lower().endswith(".csv"):
            run_log.info("Object is not a CSV; skipping.")
            continue

        # The input contract for the whole pipeline - which file to process
        sfn_input = {"bucket": bucket, "key": key}

        run_log.info("Starting Step Functions execution.")
        response = sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(sfn_input),
        )
        run_log.info(f"Execution started: {response['executionArn']}")
        started += 1

    return {"started": started}
