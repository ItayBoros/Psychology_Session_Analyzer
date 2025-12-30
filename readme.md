# Psychology Session Analyzer

![Project Architecture](Project%20Architecture.png)

A comprehensive, microservices-based platform designed to analyze psychology therapy sessions. This system ingests audio and video recordings, transcribes them, and uses advanced NLP to provide deep insights into therapy session dynamics.

## Features

-   **Secure Ingestion**: Upload therapy session recordings securely via a dedicated API.
-   **Automated Pipeline**: Event-driven architecture ensures files are processed, extracted, and analyzed asynchronously.
-   **AI Transcription**: High-accuracy transcription using AssemblyAI.
-   **Deep Analysis**: Leverages OpenAI's GPT models to generate summaries, sentiment analysis, and behavioral insights.
-   **Searchable Archive**: Query past sessions and analysis results instantly.
-   **Monitoring**: Full observability pipeline integrated with Datadog.
-   **User-Friendly Interface**: Clean and intuitive Streamlit dashboard for therapists.

## Tech Stack

-   **Backend**: Python, FastAPI
-   **Frontend**: Streamlit
-   **AI Services**: AssemblyAI, OpenAI
-   **Database & Storage**: MongoDB, MinIO (S3 compatible), Redis
-   **Messaging**: RabbitMQ
-   **Infrastructure**: Docker, Docker Compose
-   **Monitoring**: Datadog

## Prerequisites

Ensure you have the following installed:
-   Docker
-   Docker Compose

You will also need API keys for:
-   OpenAI
-   AssemblyAI
-   Datadog

## Installation & Setup

1.  **Clone the Repository**
    ```bash
    git clone <repository-url>
    cd Psychology_Session_Analyzer
    ```

2.  **Environment Configuration**
    Create a `.env` file in the root directory with the following credentials:
    ```env
    OPENAI_API_KEY=your_openai_key
    ASSEMBLYAI_API_KEY=your_assemblyai_key
    DD_API_KEY=your_datadog_api_key
    DD_SITE=datadoghq.com  # or your specific Datadog site
    ```

3.  **Launch the System**
    Start all services using Docker Compose:
    ```bash
    docker-compose up -d
    ```

## Usage

1.  **Access the Dashboard**: Open your browser and navigate to `http://localhost:8501`.
2.  **Upload a Session**: Use the upload interface to submit an audio or video file.
3.  **View Analysis**: Once processing is complete, the results (transcription, summary, insights) will be available in the 'Analysis' tab.

## Project Structure

-   `frontend/`: Streamlit user interface.
-   `upload-api/`: API for handling file uploads.
-   `media-processor/`: Extracts audio from video files.
-   `transcriber/`: Converts audio to text using AssemblyAI.
-   `nlp-analyzer/`: Generates insights using OpenAI.
-   `query-api/`: Retrieves analysis results.
-   `docker-compose.yml`: Orchestration for all services.

## Maintenance & Troubleshooting

**Reset a specific container:**
```bash
docker-compose stop
docker-compose rm -f <container name>
docker-compose build --no-cache <container name>
docker-compose up -d
```

**Stop all services:**
```bash
docker-compose down
```