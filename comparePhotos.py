import boto3
import os
import cv2 as cv
import numpy as np
from PIL import Image
from io import BytesIO
import face_recognition
from dotenv import load_dotenv
import os



load_dotenv()

# Load the AWS credentials from environment variables or AWS credentials file
s3_client = boto3.client('s3',
                         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                         region_name=os.getenv('AWS_DEFAULT_REGION'),
                         endpoint_url = os.getenv('AWS_S3_ENDPOINT'))

bucket_name = 'snap-bucket' 
prefix = 'test/faces/'  # Replace with your prefix



def check_for_face(object_key, user_image_encodings):
    # Get the object from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    image_data = response['Body'].read()

    # Open the image using PIL
    pil_image = Image.open(BytesIO(image_data))
    image = np.array(pil_image)
    image_rgb = cv.cvtColor(image, cv.COLOR_BGR2RGB)

    # Detect faces in the image
    faces = face_recognition.face_locations(image_rgb)
    face_encodings = face_recognition.face_encodings(image_rgb, faces)

    # Compare faces
    for face_encoding in face_encodings:
        matches = face_recognition.compare_faces([user_image_encodings], face_encoding, tolerance=0.45)
        if matches[0]:
            return object_key  # Return the key of the matching face image
    return None  # No match found



def list_s3_objects(bucket, prefix=''):
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in page_iterator:
        for obj in page.get('Contents'):
            yield obj['Key']



# Load the user's image and get the encodings
user_image_path = "Dharuntest.jpg"  
user_image = face_recognition.load_image_file(user_image_path)
user_image_encodings = face_recognition.face_encodings(user_image)[0]

# Iterate over the images in the S3 bucket and compare them with the user's image

for object_key in list_s3_objects(bucket_name, prefix=prefix):
    if not object_key.endswith('/'):  
        matched_key = check_for_face(object_key, user_image_encodings)
        if matched_key:
            print(f"Match found: {matched_key}")
            break
