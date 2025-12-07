import os
import json
import time
import pika
import requests
import logging
from minio import Minio

# configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("transcriber")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
ASSEMBLYAI_API_KEY = os.getenv("ASSEMBLYAI_API_KEY")

AUDIO_BUCKET = "audio"

#setup client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def upload_to_assemblyai(file_path):
    headers = {'authorization': ASSEMBLYAI_API_KEY}
    
    def read_file(path):
        with open(path, 'rb') as f:
            while True:
                data = f.read(5242880) # read 5MB chunks
                if not data:
                    break
                yield data

    logger.info("Uploading audio to AssemblyAI...")
    response = requests.post(
        'https://api.assemblyai.com/v2/upload',
        headers=headers,
        data=read_file(file_path)
    )
    
    if response.status_code != 200:
        logger.error(f"Upload Failed: {response.text}")
        response.raise_for_status()
        
    return response.json()['upload_url']

def transcribe_audio(audio_url):
    endpoint = "https://api.assemblyai.com/v2/transcript"
    
    json_payload = {
        "audio_url": audio_url,
        "speaker_labels": True
        # default lang - US english
    }
    
    headers = {
        "authorization": ASSEMBLYAI_API_KEY,
        "content-type": "application/json"
    }
    
    response = requests.post(endpoint, json=json_payload, headers=headers)
    
    return response.json()['id']

def wait_for_completion(transcript_id):
    endpoint = f"https://api.assemblyai.com/v2/transcript/{transcript_id}"
    headers = {"authorization": ASSEMBLYAI_API_KEY}
    
    while True:
        response = requests.get(endpoint, headers=headers)
        status = response.json()['status']
        
        if status == 'completed':
            return response.json()
        elif status == 'error':
            raise Exception(f"Transcription failed: {response.json()['error']}")
        
        logger.info(f"Status: {status}... waiting 5s")
        time.sleep(5)

def process_audio(ch, method, properties, body):
    try:
        message = json.loads(body)
        logger.info(f"Received job: {message}")
        
        video_id = message['video_id']
        filename_for_dashboard = message.get('filename', 'Unknown_Video')

        audio_filename = os.path.basename(message['audio_path'])
        local_path = f"/tmp/{audio_filename}"
        
        # Download
        logger.info(f"Downloading {audio_filename}...")
        minio_client.fget_object(AUDIO_BUCKET, audio_filename, local_path)
        
        #  Upload
        upload_url = upload_to_assemblyai(local_path)
        
        # Transcribe
        logger.info("Starting transcription job...")
        transcript_id = transcribe_audio(upload_url)
        
        # Wait
        result = wait_for_completion(transcript_id)
        logger.info("Transcription complete!")
        
        # Publish
        next_message = {
            "video_id": video_id,
            "transcript_text": result['text'],
            "utterances": result['utterances'],
            "filename": filename_for_dashboard
        }
        
        ch.basic_publish(
            exchange='',
            routing_key='transcription_ready',
            body=json.dumps(next_message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"CRITICAL ERROR: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

def main():
    if not ASSEMBLYAI_API_KEY:
        logger.error("Missing ASSEMBLYAI_API_KEY!")
        return

    logger.info("Waiting for RabbitMQ...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=0))
    channel = connection.channel()
    
    channel.queue_declare(queue='audio_extracted', durable=True)
    channel.queue_declare(queue='transcription_ready', durable=True)
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='audio_extracted', on_message_callback=process_audio)
    
    logger.info("Transcriber worker started...")
    channel.start_consuming()

if __name__ == "__main__":
    main()