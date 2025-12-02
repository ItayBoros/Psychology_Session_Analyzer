import os
import json
import pika
import logging
import tempfile
from minio import Minio
from moviepy.editor import VideoFileClip

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

def process_video(ch, method, properties, body):
    message = json.loads(body)
    logger.info(f"Received job: {message}")

    video_id = message['video_id']   
    file_path = message['file_path']
    filename = os.path.basename(file_path)  

    # download video from Minio to temp file
    local_video_path = f"/tmp/{filename}"
    local_audio_path = f"/tmp/{video_id}.mp3"

    try:
        logger.info(f"Downloading video {filename} from Minio...")
        client.fget_object(SOURCE_BUCKET, filename, local_video_path)
        
        # extract audio using moviepy
        logger.info(f"Extracting audio from video {filename}...")
        video = VideoFileClip(local_video_path)
        video.audio.write_audiofile(local_audio_path, logger=None)
        video.close()

        logger.info(f"Uploading {video_id}.mp3...")

        # Check if file actually exists and has size
        file_stat = os.stat(local_audio_path)
        if file_stat.st_size == 0:
            raise Exception("Generated MP3 is empty (0 bytes)!")

        with open(local_audio_path, 'rb') as file_data:
            client.put_object(
                DEST_BUCKET, 
                f"{video_id}.mp3", 
                file_data, 
                file_stat.st_size,
                content_type="audio/mpeg"
            )        
        # publish audio ready message to RabbitMQ
        next_message = {
            "video_id": video_id,
            "audio_path": f"{DEST_BUCKET}/{video_id}.mp3"
        }
        
        ch.basic_publish(
            exchange='',
            routing_key='audio_extracted',
            body=json.dumps(next_message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
        logger.info("Job done, Message published.")

        # acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"Failed to process video {filename}: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    finally:
        # clean up temp files
        if os.path.exists(local_video_path):
            os.remove(local_video_path)
        if os.path.exists(local_audio_path):
            os.remove(local_audio_path)
        
def main():
    logger.info("Starting media processor...")
    logger.info("Waiting for RabbitMQ...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue='video_uploaded', durable=True)
    channel.queue_declare(queue='audio_extracted', durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='video_uploaded', on_message_callback=process_video)

    logger.info("Worker started. Waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    main()