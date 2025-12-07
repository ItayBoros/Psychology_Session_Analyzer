import os
import json
import time
import aio_pika
import httpx
import logging
from minio import Minio
import asyncio
from functools import partial

# configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("transcriber")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
ASSEMBLYAI_API_KEY = os.getenv("ASSEMBLYAI_API_KEY")

AUDIO_BUCKET = "audio"
INPUT_QUEUE = "audio_extracted"
OUTPUT_QUEUE = "transcription_ready"

#setup client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

async def upload_to_assemblyai(file_path):
    headers = {'authorization': ASSEMBLYAI_API_KEY}

    def read_file():
        with open(file_path, 'rb') as f:
            return f.read()

    file_data = await asyncio.to_thread(read_file)
    logger.info("Uploading audio to AssemblyAI...")
    async with httpx.AsyncClient() as client:
        response = await client.post(
            'https://api.assemblyai.com/v2/upload',
            headers=headers,
            content=file_data
        )

    if response.status_code != 200:
        logger.error(f"Upload Failed: {response.text}")
        response.raise_for_status()

    return response.json()['upload_url']

async def transcribe_audio(audio_url):
    endpoint = "https://api.assemblyai.com/v2/transcript"

    json_payload = {
        "audio_url": audio_url,
        "speaker_labels": True
    }

    headers = {
        "authorization": ASSEMBLYAI_API_KEY,
        "content-type": "application/json"
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(endpoint, json=json_payload, headers=headers)

    return response.json()['id']

async def wait_for_completion(transcript_id):
    endpoint = f"https://api.assemblyai.com/v2/transcript/{transcript_id}"
    headers = {"authorization": ASSEMBLYAI_API_KEY}

    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(endpoint, headers=headers)
            status = response.json()['status']

            if status == 'completed':
                return response.json()
            elif status == 'error':
                raise Exception(f"Transcription failed: {response.json()['error']}")

            logger.info(f"Status: {status}... waiting 5s")
            await asyncio.sleep(5)

# NOTE: pub_channel is passed in; DO NOT use message.channel or declare queues here
async def process_audio(message: aio_pika.IncomingMessage, pub_channel: aio_pika.abc.AbstractChannel):
    async with message.process():
        local_path = ""
        try:
            body = message.body.decode()
            data = json.loads(body)
            logger.info(f"Received job: {data}")

            video_id = data['video_id']
            filename_for_dashboard = data.get('filename', 'Unknown_Video')
            audio_filename = os.path.basename(data['audio_path'])

            local_path = f"/tmp/{audio_filename}"

            # Download
            logger.info(f"Downloading {audio_filename}...")
            await asyncio.to_thread(
                minio_client.fget_object,
                AUDIO_BUCKET,
                audio_filename,
                local_path
            )

            # Upload
            upload_url = await upload_to_assemblyai(local_path)

            # Transcribe
            logger.info("Starting transcription job...")
            transcript_id = await transcribe_audio(upload_url)

            # Wait
            result = await wait_for_completion(transcript_id)
            logger.info("Transcription complete!")

            # Publish to next queue using pub_channel
            next_message = {
                "video_id": video_id,
                "transcript_text": result.get('text', ""),
                "utterances": result.get('utterances', []),
                "filename": filename_for_dashboard
            }

            await pub_channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(next_message).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=OUTPUT_QUEUE
            )

        except Exception as e:
            logger.exception("CRITICAL ERROR while processing job")
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)

async def main():
    if not ASSEMBLYAI_API_KEY:
        logger.error("Missing ASSEMBLYAI_API_KEY!")
        return
    while True:
        try:
            logger.info("Waiting for RabbitMQ...")
            connection = await aio_pika.connect_robust(f"amqp://guest:guest@{RABBITMQ_HOST}/")
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=1)

                # declare queues once here
                await channel.declare_queue(INPUT_QUEUE, durable=True)
                await channel.declare_queue(OUTPUT_QUEUE, durable=True)

                queue = await channel.get_queue(INPUT_QUEUE)
                logger.info("Transcriber worker started. Waiting...")

                await queue.consume(partial(process_audio, pub_channel=channel))

                await asyncio.Future()
        except Exception as e:
            logger.error(f"Connection failed: {e}. Retrying in 5s...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
``