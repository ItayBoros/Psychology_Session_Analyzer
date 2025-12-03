import os
import json
import hashlib
import time
import pika
import logging
import redis
from pymongo import MongoClient
from openai import OpenAI
from pika import exceptions as pika_exceptions

#configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nlp-analyzer")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

#connect clients
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)

client_mongo = MongoClient(MONGO_URI)
db = client_mongo["psychology_db"]
collection = db["sessions"]

openai_client = OpenAI(api_key=OPENAI_API_KEY)

def generate_cache_key(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def analyze_with_llm(utterances):
    
    # prepare the dialogue for the prompt
    dialogue_text = ""
    for u in utterances:
        # limit very long sentences
        clean_text = u['text'][:500] 
        dialogue_text += f"{u['speaker']}: {clean_text}\n"

    # system Prompt
    system_prompt = """
        You are an expert Clinical Psychologist AI. Your task is to analyze a therapy session transcript line-by-line to extract clinical insights.

        STRICT INSTRUCTIONS:

        1. IDENTIFY ROLES: 
        Analyze speech patterns to determine who is the 'Therapist' (asks questions, guides) and who is the 'Patient' (shares feelings, answers).

        2. EMOTIONAL ARC (New): 
        Divide the session roughly into 4 chronological quarters (Start, Early-Mid, Late-Mid, End).
        Identify the DOMINANT emotion of the PATIENT in each quarter to show their emotional journey.

        3. KEY INTERVENTIONS (New):
        Identify exactly 3 key moments where the Therapist said something that caused a significant reaction.
        - trigger_topic: What did the therapist ask about?
        - patient_reaction: Did the patient open up (Positive) or shut down/become defensive (Negative)?
        - insight: A short clinical note on why this happened.

        4. GRANULAR ANALYSIS (Existing): 
        For every single utterance, assign a 'Topic' and an 'Emotion'.
        
        Use these standard lists:
        - Topics: [Family, Work, Relationships, Anxiety, Depression, Self-Esteem, Trauma, Medication, Daily Routine, Sleep]
        - Emotions: [Happy, Sad, Angry, Anxious, Neutral, Hopeful, Frustrated, Confused, Guilt, Shame]

        5. OUTPUT FORMAT: Return ONLY valid JSON with this exact structure:
        {
            "roles": {
                "Speaker A": "Therapist",
                "Speaker B": "Patient"
            },
            "emotional_profile": [
                {"phase": "Start", "emotion": "Anxious"},
                {"phase": "Early-Mid", "emotion": "Sad"},
                {"phase": "Late-Mid", "emotion": "Neutral"},
                {"phase": "End", "emotion": "Hopeful"}
            ],
            "key_interventions": [
                {
                    "trigger_topic": "Family",
                    "patient_reaction": "Negative",
                    "insight": "Patient became defensive when father was mentioned."
                }
            ],
            "analysis": [
                {
                    "speaker": "Speaker B",
                    "text": "My mom makes me happy",
                    "topic": "Family",
                    "emotion": "Happy"
                }
            ]
        }
    """

    try:
        response = openai_client.chat.completions.create(
            model="gpt-5-nano-2025-08-07",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Here is the session transcript:\n\n{dialogue_text}"}
            ],
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        logger.error(f"OpenAI API Call failed: {e}")
        raise e
    
def process_analysis(ch, method, properties, body):
    try:
        message = json.loads(body)
        logger.info(f"Received job for video: {message['video_id']}")
        
        video_id = message['video_id']
        transcript_text = message['transcript_text']
        utterances = message['utterances']
        
        # cache check
        cache_key = generate_cache_key(transcript_text)
        cached_result = redis_client.get(cache_key)
        
        if cached_result:
            logger.info("Cache HIT! Using previous analysis from Redis.")
            analysis_result = json.loads(cached_result)
        else:
            logger.info("Cache MISS. Calling OpenAI...")
            analysis_result = analyze_with_llm(utterances)
            
            # Save to Redis
            redis_client.setex(cache_key, 86400, json.dumps(analysis_result))

        # save to mongo db
        final_document = {
            "video_id": video_id,
            "raw_transcript": transcript_text,
            "timestamp": message.get("timestamp", None),
            "roles_identified": analysis_result.get("roles", {}),
            "emotional_profile": analysis_result.get("emotional_profile", []), 
            "key_interventions": analysis_result.get("key_interventions", []),
            "sentence_analysis": analysis_result.get("analysis", [])
        }
        
        collection.update_one(
            {"video_id": video_id}, 
            {"$set": final_document}, 
            upsert=True
        )
        logger.info(f"Successfully saved analysis to MongoDB for {video_id}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        try:
            if getattr(ch, "is_open", False):
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception as ack_err:
            logger.error(f"Failed to nack message after error: {ack_err}")

def main():
    if not OPENAI_API_KEY:
        logger.error("Missing OPENAI_API_KEY! Please check your .env file.")
        return

    connection =None

    while True:
        try:
            logger.info("Connecting to RabbitMQ...")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST)
            )
            channel = connection.channel()

            channel.queue_declare(queue='transcription_ready', durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue='transcription_ready',
                on_message_callback=process_analysis
            )

            logger.info("NLP Analyzer started. Waiting for messages...")
            channel.start_consuming()

        except (pika_exceptions.StreamLostError,
                pika_exceptions.AMQPConnectionError,
                pika_exceptions.ConnectionWrongStateError) as e:
            logger.error(f"RabbitMQ connection lost: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)
            continue

        except KeyboardInterrupt:
            logger.info("NLP Analyzer shutting down by KeyboardInterrupt...")
            try:
                if connection and not connection.is_closed:
                    connection.close()
            except Exception:
                pass
            break

        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(5)
            continue

if __name__ == "__main__":
    main()