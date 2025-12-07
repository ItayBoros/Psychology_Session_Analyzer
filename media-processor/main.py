import os
import json
import aio_pika
import logging
import tempfile
from minio import Minio
from moviepy.editor import VideoFileClip
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("media_processor")

# configuration from environment variables from Docker
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

SOURCE_BUCKET = "videos"
DEST_BUCKET = "audio"

# connect to Minio
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)
if not client.bucket_exists(DEST_BUCKET):
    client.make_bucket(DEST_BUCKET)
    logger.info(f"Created bucket: {DEST_BUCKET}")

def convert_video_to_audio_sync(video_id, minio_object_name):
    local_video_path = f"/tmp/{minio_object_name}"
    local_audio_path = f"/tmp/{video_id}.mp3"

    try:
        logger.info(f"Downloading {minio_object_name}...")
        client.fget_object(SOURCE_BUCKET, minio_object_name, local_video_path)
        
        logger.info(f"Extracting audio...")
        video = VideoFileClip(local_video_path)
        video.audio.write_audiofile(local_audio_path, logger=None)
        video.close()

        file_stat = os.stat(local_audio_path)
        if file_stat.st_size == 0:
            raise Exception("Generated MP3 is empty!")

        logger.info(f"Uploading {video_id}.mp3...")
        with open(local_audio_path, 'rb') as file_data:
            client.put_object(
                DEST_BUCKET, 
                f"{video_id}.mp3", 
                file_data, 
                file_stat.st_size,
                content_type="audio/mpeg"
            )
        return f"{DEST_BUCKET}/{video_id}.mp3"

    finally:
        # cleanup
        if os.path.exists(local_video_path): os.remove(local_video_path)
        if os.path.exists(local_audio_path): os.remove(local_audio_path)

async def process_video_message(message: aio_pika.IncomingMessage):
    async with message.process():
        try:
            body = message.body.decode()
            data = json.loads(body)
            logger.info(f"Received job: {data}")

            video_id = data['video_id']
            file_path = data['file_path']
            filename_for_dashboard = data.get('filename', 'Unknown_Video')
            minio_object_name = os.path.basename(file_path)

            audio_path = await asyncio.to_thread(
                convert_video_to_audio_sync, 
                video_id, 
                minio_object_name
            )

            #publish next message
            channel = message.channel
            
            #ensure next queue exists
            await channel.declare_queue("audio_extracted", durable=True)

            next_message = {
                "video_id": video_id,
                "audio_path": audio_path,
                "filename": filename_for_dashboard 
            }

            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(next_message).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key="audio_extracted"
            )
            logger.info("Job done. Message published.")

        except Exception as e:
            logger.error(f"Failed to process video: {e}")


async def main():
    while True:
        try:
            logger.info("Connecting to RabbitMQ (Async)...")
            connection = await aio_pika.connect_robust(f"amqp://guest:guest@{RABBITMQ_HOST}/")
            
            async with connection:
                channel = await connection.channel()
                
                #setup queue
                queue = await channel.declare_queue("video_uploaded", durable=True)
                await channel.set_qos(prefetch_count=1)

                logger.info("Media Processor started. Waiting...")
                # Start consuming
                await queue.consume(process_video_message)
                # keep running
                await asyncio.Future()

        except Exception as e:
            logger.error(f"Connection failed: {e}. Retrying in 5s...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    main()