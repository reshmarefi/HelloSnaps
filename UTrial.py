import boto3
import cv2 as cv
import face_recognition
import numpy as np
from PIL import Image
from io import BytesIO
import json
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
s3_client = boto3.client('s3',
                        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                        region_name=os.getenv('AWS_DEFAULT_REGION'),
                        endpoint_url = os.getenv('AWS_S3_ENDPOINT'))
print(os.getenv('AWS_ACCESS_KEY_ID'))
print(os.getenv('AWS_SECRET_ACCESS_KEY'))
print(os.getenv('AWS_DEFAULT_REGION'))
bucket_name = 'snap-bucket' 
prefix = 'SsG68NPSBH7Pw_8clXnin'

# Global variables for face tracking
unique_faces = []
unique_details = []
unique_count = 0
image_count = 0
Map = {}
faceMap = {}


def uploadUniqueFace(name, buffer, details):
    detailsList = details.tolist()
    detailsStr = json.dumps(detailsList)
    object_key = f"{prefix}/people/{name}.jpg"  
    # Upload the image to the S3 bucket
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=buffer,ACL='public-read')

def unique_face_identifier(object_key, PhotoId):
    global unique_count
    global image_count
    
    image_count += 1

    # Get the object from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    image_data = response['Body'].read()

    # Open the image using PIL
    bytes_io = BytesIO(image_data)
    bytes_io.seek(0)  # Rewind the BytesIO object
    pil_image = Image.open(bytes_io)
    image = np.array(pil_image)
    image_rgb = cv.cvtColor(image, cv.COLOR_BGR2RGB)

    # Stores all faces
    faces = face_recognition.face_locations(image_rgb)
    print(f"Number of faces in pic {image_count} = {len(faces)}")
    
    for face in faces:
        a, b, c, d = face
        details = face_recognition.face_encodings(image_rgb, [face])[0]
        unique = True
        if len(unique_faces) == 0:
            unique_faces.append(face)
            unique_details.append(details)
            unique_count += 1
            Map[unique_count] = ([details],[PhotoId])
            if(PhotoId in faceMap):
                faceMap[PhotoId].append(unique_count)
            else:
                faceMap[PhotoId] = [unique_count]
            ret, buffer = cv.imencode(".jpg", image_rgb[a:c, d:b])
            uploadUniqueFace(unique_count, buffer.tobytes(), details)
        else:
            for key, value in Map.items():
                if face_recognition.compare_faces([value[0][0]], details, tolerance=0.60)[0]:
                    Map[key][0].append(details)
                    Map[key][1].append(PhotoId)
                    if(PhotoId in faceMap):
                        faceMap[PhotoId].append(key)
                    else:
                        faceMap[PhotoId] = [key]
                    unique = False
                    break
            
            if unique:
                unique_faces.append(face)
                unique_details.append(details)
                unique_count += 1
                Map[unique_count] = ([details],[PhotoId])
                if(PhotoId in faceMap):
                    faceMap[PhotoId].append(unique_count)
                else:
                    faceMap[PhotoId] = [unique_count]
                ret, buffer = cv.imencode(".jpg", image_rgb[a:c, d:b])
                uploadUniqueFace(unique_count, buffer.tobytes(), details)

def list_s3_objects(bucket, prefix = ''):
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in page_iterator:
        for obj in page['Contents']:
            yield obj['Key']
            
if __name__ == "__main__":
    # Iterate over the list of objects in S3 and process them
    for object_key in list_s3_objects(bucket_name, prefix=prefix.strip('/')):
        if 'people/' not in object_key and not object_key.endswith('/'):
            unique_face_identifier(object_key, object_key)  # Using  object_key as PhotoId for this example

    for unique_id, details in Map.items():
        print(f"Unique ID: {unique_id}, Photo IDs: {details[1]}")
    
    print("Face mapping to Photo IDs:")
    for photo_id, face_ids in faceMap.items():
        print(f"Photo ID: {photo_id}, Face IDs: {face_ids}")
