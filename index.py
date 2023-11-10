import boto3
import psycopg2
import cv2 as cv
import face_recognition
import numpy as np
from PIL import Image
from io import BytesIO
import os
import requests
from dotenv import load_dotenv
import json

# Global variables to keep track of unique faces
unique_faces_encodings = []
unique_faces_count = 0
prefix = ""
unique_face_to_photos_map = {}
photo_to_unique_faces_map = {}
faceMap = {}


load_dotenv()

# Assuming you have the AWS credentials and database settings in your .env file
s3_client = boto3.client('s3',
                        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                        region_name=os.getenv('AWS_DEFAULT_REGION'),
                        endpoint_url='https://blr1.digitaloceanspaces.com',
                        verify=False)



db_params = {
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}


def update_face_map_in_media_table(conn, faceMap,workspace):
    cursor = conn.cursor()
    try:
        for image_id, face_ids in faceMap.items():
            # Convert list of face_ids to a format PostgreSQL expects for an array
            face_ids_array = "{" + ", ".join(face_ids) + "}"
            update_query = """
            UPDATE public."Media"
            SET tags = %s
            WHERE id = %s AND workspace = %s;
            """
            cursor.execute(update_query, (face_ids_array, image_id, workspace))
            print(f"Updated image_id: {image_id}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        cursor.close()


def unique_face_identifier(image_url, image_id):
    global unique_faces_encodings
    global unique_faces_count
    global unique_face_to_photos_map
    global photo_to_unique_faces_map
    global faceMap
    global prefix
    prefix = image_url.split('/')[-2]

    try:
        response = requests.get(image_url)
        image = face_recognition.load_image_file(BytesIO(response.content))
        face_locations = face_recognition.face_locations(image, number_of_times_to_upsample=1)
        face_encodings = face_recognition.face_encodings(image, face_locations)

        for face_encoding, face_location in zip(face_encodings, face_locations):
            is_new_face = not any(face_recognition.compare_faces(unique_faces_encodings, face_encoding, tolerance=0.6))
            if is_new_face:
                unique_faces_encodings.append(face_encoding)
                unique_faces_count += 1
                unique_face_id = f"face_{unique_faces_count}"
                unique_face_to_photos_map[unique_face_id] = unique_face_to_photos_map.get(unique_face_id, []) + [image_id]
                photo_to_unique_faces_map[image_id] = photo_to_unique_faces_map.get(image_id, []) + [unique_face_id]
                
                # Crop the face from the image
                top, right, bottom, left = face_location
                face_image = Image.fromarray(image[top:bottom, left:right])

                # Convert the PIL Image to a buffer to upload to S3
                img_buffer = BytesIO()
                face_image.save(img_buffer, format="JPEG")
                img_buffer.seek(0)

                # Create a file name for the face
                face_filename = f"unique_face_{image_id}_{unique_faces_count}.jpg"

                if image_id in faceMap:
                    faceMap[image_id].add(face_filename)
                else:
                    faceMap[image_id] = {face_filename}
                # Upload the face image to S3
                try:
                    s3_client.put_object(
                        Bucket=s3_bucket_name,
                        Key=f"{prefix}/people/{face_filename}",
                        Body=img_buffer.getvalue(),
                        ContentType='image/jpeg',
                        ACL='public-read'
                    )
                    print(f"Uploaded {face_filename} to S3 bucket {s3_bucket_name}")
                except Exception as e:
                    print(f"Failed to upload {face_filename} to S3: {e}")
            
            else:
            # If the face is not new, find the existing unique ID and add it to the faceMap for this photo
                
                for known_face_enc, known_face_id in zip(unique_faces_encodings, unique_face_to_photos_map):
                    if face_recognition.compare_faces([known_face_enc], face_encoding, tolerance=0.6)[0]:
                        existing_face_filename = f"unique_face_{image_id}_{known_face_id.split('_')[-1]}.jpg"
                        if image_id in faceMap:
                            faceMap[image_id].add(existing_face_filename)
                        else:
                            faceMap[image_id] = {existing_face_filename}
                        break
                    

    except Exception as e:
        print(f"Error processing image ID {image_id}: {e}")

# Main function to process images and update the database
def process_workspace_images(workspace):
    

    # Connect to the database
    conn = psycopg2.connect(**db_params)
    face_map = {}
    query = 'SELECT * FROM public."Media" WHERE workspace = %s;'

    try:
        cursor = conn.cursor()
        cursor.execute(query, [workspace])
        rows = cursor.fetchall()

        url_prefix = "https://blr1.digitaloceanspaces.com/snap-bucket/"

        for row in rows:
            image_url = url_prefix + row[2]
            unique_face_identifier(image_url, row[0])

        update_face_map_in_media_table(conn, face_map,workspace)
    except Exception as e:
        print(f"Database or processing error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

# Call the main function with the workspace name
if __name__ == "__main__":
    import sys
    s3_bucket_name = 'snap-bucket'

    # Expect the workspace name as the first argument to the script
    
    workspace_name = "Cs_bXqRsdhJXdCDsB7AoA"
    process_workspace_images(workspace_name)
    print("Done")
    
    

