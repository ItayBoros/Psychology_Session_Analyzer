import os 
import json
import logging
from datetime import datetime
from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.concurrency import run_in_threadpool
import aio_pika
import uuid
import io
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
async def publish_message(video_id: str, original_filename: str, minio_path: str):
        try:
            connection = await aio_pika.connect_robust(f"amqp://guest:guest@{RABBITMQ_HOST}/")            
            
            async with connection:
                channel = await connection.channel()
                # Declare the queue
                queue = await channel.declare_queue("video_uploaded", durable=True)

                message = {
                "video_id": video_id,
                "filename": original_filename,
                "file_path": minio_path,
                "timestamp": datetime.now().isoformat()
                }
                await channel.default_exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(message).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    ),
                    routing_key="video_uploaded"
                )
            
            logger.info(f"Published message to RabbitMQ: {message}")
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
        data_stream = io.BytesIO(file_data)
        await run_in_threadpool(
            client.put_object,
            BUCKET_NAME,
            new_filename,
            data_stream,
            length=len(file_data),
            content_type=file.content_type
        )
        logger.info(f"Uploaded {file.filename} as {new_filename}")


        await publish_message(
            file_uuid,
            file.filename,
            f"{BUCKET_NAME}/{new_filename}"
        )

        return {
            "message": "Video uploaded successfully",
            "video_id": file_uuid,
            "filename": file.filename
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