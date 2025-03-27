import os
import time
import logging
from io import BytesIO

import boto3
import torch
from botocore.exceptions import ClientError
from facenet_pytorch import MTCNN, InceptionResnetV1
from PIL import Image


# AWS resource configurations
ID = "YourID"
IAM = "IAMNumber"
AWS_REGION = "us-east-1"
INPUT_BUCKET = f"{ID}-in-bucket"
OUTPUT_BUCKET = f"{ID}-out-bucket"
REQUEST_QUEUE = f"{ID}-req-queue"
RESPONSE_QUEUE = f"{ID}-resp-queue"

# Initialize AWS service clients
s3 = boto3.client("s3", region_name=AWS_REGION)
sqs = boto3.client("sqs", region_name=AWS_REGION)

# Define SQS queue URLs
REQ_QUEUE_URL = f"https://sqs.us-east-1.amazonaws.com/{IAM}/{REQUEST_QUEUE}"
RESP_QUEUE_URL = f"https://sqs.us-east-1.amazonaws.com/{IAM}/{RESPONSE_QUEUE}"

# Load face detection and recognition models
mtcnn = MTCNN(image_size=240, margin=0, min_face_size=20)
face_model = InceptionResnetV1(pretrained="vggface2").eval()

# Load stored face embeddings and names
face_db = torch.load("data.pt")
embeddings, names = face_db[0], face_db[1]


def identify_face(image_bytes):
    """Processes an image to detect and recognize a face."""
    image = Image.open(BytesIO(image_bytes))

    with torch.no_grad():
        face_tensor, probability = mtcnn(image, return_prob=True)
        face_embedding = face_model(face_tensor.unsqueeze(0))

    # Calculate distances between detected face and stored embeddings
    distances = [torch.dist(face_embedding, ref_emb).item() for ref_emb in embeddings]

    best_match_idx = distances.index(min(distances))
    matched_name = names[best_match_idx]
    match_distance = min(distances)

    print(f"Identified face: {matched_name} with distance {match_distance}")
    return matched_name, match_distance


def process_requests():
    """Handles image processing requests from SQS."""
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=REQ_QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=5
            )
        except ClientError as error:
            print(f"Failed to fetch message from SQS: {error}")
            time.sleep(1)
            continue

        messages = response.get("Messages", [])
        if not messages:
            continue

        for message in messages:
            file_name = message["Body"].strip()
            base_name = os.path.splitext(file_name)[0]
            print(f"Processing image: {file_name}")

            try:
                image_obj = s3.get_object(Bucket=INPUT_BUCKET, Key=file_name)
                image_content = image_obj["Body"].read()
                print("Image successfully retrieved from S3")
            except ClientError:
                sqs.delete_message(
                    QueueUrl=REQ_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"]
                )
                continue

            # Recognize face in the image
            matched_name, _ = identify_face(image_content)

            try:
                s3.put_object(Bucket=OUTPUT_BUCKET, Key=base_name, Body=matched_name)
            except ClientError as error:
                print(f"Error saving results to S3: {error}")

            response_message = f"{base_name}:{matched_name}"
            try:
                sqs.send_message(QueueUrl=RESP_QUEUE_URL, MessageBody=response_message)
            except ClientError as error:
                print(f"Failed to send response to SQS: {error}")

            try:
                sqs.delete_message(
                    QueueUrl=REQ_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"]
                )
                print("Processed request removed from queue")
            except ClientError as error:
                print(f"Failed to delete message from SQS: {error}")

        time.sleep(0.5)


if __name__ == "__main__":
    process_requests()
