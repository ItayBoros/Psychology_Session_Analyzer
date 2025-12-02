import os 
import json
import logging
from fastapi import FastAPI, UploadFile, HTTPException
import pika
import uuid
from minio import Minio
from minio.error import S3Error

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("upload-api")

app = FastAPI()

# configuration from environment variables from Docker
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
BUCKET_NAME = "videos"

#connect to Minio
def get_minio_client():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        logger.info(f"Created bucket: {BUCKET_NAME}")
    return client

#connect to RabbitMQ
def publish_message(video_id: str, original_filename: str, minio_path: str):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            
            # Declare the queue
            channel.queue_declare(queue='video_tasks', durable=True)

            message = {
            "video_id": video_id,
            "original_filename": original_filename,
            "file_path": minio_path
            }
            
            channel.basic_publish(
                exchange='',
                routing_key='video_uploaded',
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                ))
            logger.info(f"Published message to RabbitMQ: {message}")
            connection.close()
        except Exception as e:
            logger.error(f"Failed to publish message to RabbitMQ: {e}")
            raise e
    
# Upload endpoint
@app.post("/upload/")
async def upload_video(file: UploadFile):
    client = get_minio_client()

    file_uuid = str(uuid.uuid4())

    file_extention = os.path.splitext(file.filename)[1]

    new_filename = f"{file_uuid}{file_extention}"

    try:
        file_data = await file.read()
        import io
        data_stream = io.BytesIO(file_data)
        client.put_object(
            BUCKET_NAME,
            new_filename,
            data_stream,
            length=len(file_data),
            content_type=file.content_type
        )
        logger.info(f"Uploaded {file.filename} as {new_filename}")

        # Trigger Event with both the UUID and the real path
        minio_path = f"{BUCKET_NAME}/{new_filename}"
        publish_message(file_uuid, file.filename, minio_path)

        return {
            "message": "Video uploaded successfully",
            "video_id": file_uuid,
            "original_name": file.filename
        }
    except S3Error as e:
        logger.error(f"MinIO Error: {e}")
        raise HTTPException(status_code=500, detail="Storage Error")
    except Exception as e:
        logger.error(f"genneral Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {"status": "ok"}