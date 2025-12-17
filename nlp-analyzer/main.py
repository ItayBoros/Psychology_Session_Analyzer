import os
import json
import hashlib
import time
import aio_pika
import logging
import redis.asyncio as redis 
from motor.motor_asyncio import AsyncIOMotorClient
from openai import AsyncOpenAI
import asyncio
import sys

# Configure logging
logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout
                    )
logger = logging.getLogger("nlp-analyzer")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

#connect clients
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

client_mongo = AsyncIOMotorClient(MONGO_URI)
db = client_mongo["psychology_db"]
collection = db["sessions"]

openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

def generate_cache_key(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()

async def analyze_with_llm(utterances):
    
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
        response = await openai_client.chat.completions.create(
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
    
async def process_analysis(message: aio_pika.IncomingMessage):
    async with message.process():
        try:
            body = message.body.decode()
            data = json.loads(body)
            logger.info(f"Received job for video: {data['video_id']}")
            
            video_id = data['video_id']
            transcript_text = data['transcript_text']
            utterances = data['utterances']
            
            # cache check
            cache_key = generate_cache_key(transcript_text)
            cached_result = await redis_client.get(cache_key)
            
            if cached_result:
                logger.info("Cache HIT! Using previous analysis from Redis.")
                analysis_result = json.loads(cached_result)
            else:
                logger.info("Cache MISS. Calling OpenAI...")
                analysis_result = await analyze_with_llm(utterances)
                
                # Save to Redis
                await redis_client.setex(cache_key, 86400, json.dumps(analysis_result))

            # save to mongo db  
            final_document = {
                "video_id": video_id,
                "filename": data.get("filename", "Unknown"),
                "raw_transcript": transcript_text,
                "timestamp": data.get("timestamp", None),
                "roles_identified": analysis_result.get("roles", {}),
                "emotional_profile": analysis_result.get("emotional_profile", []), 
                "key_interventions": analysis_result.get("key_interventions", []),
                "sentence_analysis": analysis_result.get("analysis", [])
            }
            
            await collection.update_one(
                {"video_id": video_id}, 
                {"$set": final_document}, 
                upsert=True
            )
            logger.info(f"Successfully saved analysis to MongoDB for {video_id}")

        except Exception as e:
            logger.error(f"Analysis failed: {e}")

async def main():
    if not OPENAI_API_KEY:
        logger.error("Missing OPENAI_API_KEY! Please check your .env file.")
        return

    connection =None

    while True:
        try:
            logger.info("Connecting to RabbitMQ...")
            connection = await aio_pika.connect_robust(f"amqp://guest:guest@{RABBITMQ_HOST}/")
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=1)
                queue = await channel.declare_queue("transcription_ready", durable=True)
                await queue.consume(process_analysis)


                await asyncio.Future()

        except Exception as e:
            logger.error(f"Connection failed: {e}. Retrying in 5s...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())