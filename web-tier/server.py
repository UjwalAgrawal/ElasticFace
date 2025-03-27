import os
import time
import threading
import logging
from flask import Flask, request
import boto3
from botocore.exceptions import ClientError

# AWS Configuration
ID = "YourID"
AWS_REGION = "us-east-1"
S3_BUCKET_NAME = f"{ID}-in-bucket"
IAM = "IAMNumber"
REQUEST_QUEUE = f"{ID}-req-queue"
RESPONSE_QUEUE = f"{ID}-resp-queue"
REQ_QUEUE_URL = f"https://sqs.us-east-1.amazonaws.com/{IAM}/{REQUEST_QUEUE}"
RESP_QUEUE_URL = f"https://sqs.us-east-1.amazonaws.com/{IAM}/{RESPONSE_QUEUE}"
POLL_INTERVAL_SEC = 0.2
MAX_RESULT_WAIT_SEC = 60

# Initialize AWS Clients
s3_client = boto3.client("s3", region_name=AWS_REGION)
sqs_client = boto3.client("sqs", region_name=AWS_REGION)

# Flask Application
app = Flask(__name__)
prediction_results = {}


def background_polling():
    """Continuously polls the SQS response queue and stores results."""
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=RESP_QUEUE_URL, MaxNumberOfMessages=10, WaitTimeSeconds=5
            )
            for msg in response.get("Messages", []):
                body = msg["Body"].split(":", 1)
                if len(body) == 2:
                    prediction_results[body[0]] = body[1]
                sqs_client.delete_message(
                    QueueUrl=RESP_QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"]
                )
        except Exception as ex:
            print(f"Polling error: {ex}")
        time.sleep(POLL_INTERVAL_SEC)


threading.Thread(target=background_polling, daemon=True).start()


@app.route("/", methods=["POST"])
def handle_prediction_request():
    """Handles image upload, sends SQS request, and waits for response."""
    if "inputFile" not in request.files:
        return "Missing inputFile", 400

    uploaded_file = request.files["inputFile"]
    if not uploaded_file.filename:
        return "Empty filename", 400

    try:
        s3_client.upload_fileobj(uploaded_file, S3_BUCKET_NAME, uploaded_file.filename)
        sqs_client.send_message(
            QueueUrl=REQ_QUEUE_URL, MessageBody=uploaded_file.filename
        )
    except ClientError as err:
        return f"AWS Error: {err}", 500

    base_filename = os.path.splitext(uploaded_file.filename)[0]
    start_time = time.time()
    while time.time() - start_time < MAX_RESULT_WAIT_SEC:
        if base_filename in prediction_results:
            return f"{base_filename}:{prediction_results.pop(base_filename)}", 200
        time.sleep(0.5)

    return "Timeout waiting for recognition result", 504


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
