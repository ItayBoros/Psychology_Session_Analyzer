import streamlit as st
import requests
import time
import pandas as pd
import plotly.express as px

# use the internal docker service names
UPLOAD_API_URL = "http://upload-api:8000"
QUERY_API_URL = "http://query-api:8000"

st.set_page_config(page_title="Psychology AI", layout="wide")

#side bar 
st.sidebar.title("PsychoAnalyzer")
page = st.sidebar.radio("Navigation", ["Upload Session", "Analysis Dashboard"])

def map_emotion_to_color(emotion):
    colors = {
        "Happy": "#2ecc71", "Hopeful": "#27ae60", # Green
        "Neutral": "#95a5a6", # Grey
        "Sad": "#3498db", "Confused": "#2980b9", # Blue
        "Anxious": "#e67e22", "Frustrated": "#d35400", # Orange
        "Angry": "#e74c3c", "Shame": "#c0392b", "Guilt": "#8e44ad" # Red/Purple
    }
    return colors.get(emotion, "#95a5a6")

# upload page
if page == "Upload Session":
    st.header("Upload New Therapy Session")
    st.write("Upload a video/audio file to begin the AI analysis pipeline.")

    uploaded_file = st.file_uploader("Choose a video file", type=["mp4", "mov", "avi", "mp3"])

    if uploaded_file is not None:
        if st.button("Start Analysis"):
            with st.spinner("Uploading to Server..."):
                files = {"file": (uploaded_file.name, uploaded_file, uploaded_file.type)}
                try:
                    # upload
                    response = requests.post(f"{UPLOAD_API_URL}/upload", files=files)
                    response.raise_for_status()
                    data = response.json()
                    video_id = data['video_id']
                    
                    st.success(f"Upload Complete! ID: {video_id}")
                    
                    # Polling Loop
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    status_text.text("⏳ Status: Transcribing & Analyzing...")
                    
                    for i in range(60):
                        time.sleep(5)
                        progress_bar.progress((i + 1) / 60)
                        
                        # check if analysis exists
                        check_res = requests.get(f"{QUERY_API_URL}/analysis/{video_id}")
                        if check_res.status_code == 200:
                            progress_bar.progress(100)
                            status_text.success("Analysis Complete!")
                            st.balloons()
                            st.info(f"Go to the 'Analysis Dashboard' and enter ID: {video_id}")
                            break
                    else:
                        status_text.error("Timeout: The analysis is taking longer than expected.")
                        
                except Exception as e:
                    st.error(f"Error connecting to backend: {e}")

# dashboard page
elif page == "Analysis Dashboard":
    st.header("📊 Session Analysis")
    
    try:
        list_res = requests.get(f"{QUERY_API_URL}/list")
        if list_res.status_code == 200:
            sessions = list_res.json()
            session_ids = [s['video_id'] for s in sessions]
            selected_id = st.selectbox("Select a Session", session_ids)
        else:
            st.error("Could not fetch session list.")
            selected_id = st.text_input("Enter Video ID manually")
    except:
        st.error("Query API is offline.")
        selected_id = None

    if selected_id:
        if st.button("Load Report"):
            try:
                # fetch Data
                res = requests.get(f"{QUERY_API_URL}/analysis/{selected_id}")
                if res.status_code != 200:
                    st.error("Analysis not found.")
                    st.stop()
                    
                data = res.json()
                
                # overview
                st.divider()
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("Roles Identified")
                    roles = data.get("roles_identified", {})
                    for speaker, role in roles.items():
                        st.write(f"**{speaker}:** {role}")
                
                with col2:
                    st.subheader("Session Meta")
                    st.write(f"**ID:** {data['video_id']}")
                    st.write(f"**Date:** {data.get('timestamp', 'N/A')}")

                # emotinal erc
                st.divider()
                st.subheader("📈 Emotional Arc")
                arc = data.get("emotional_profile", [])
                
                if arc:
                    df_arc = pd.DataFrame(arc)
                    fig = px.line(df_arc, x="phase", y="emotion", title="Dominant Emotion per Quarter", markers=True)
                    fig.update_traces(line_color='#8884d8', line_width=4)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No Emotional Arc data found.")

                # interventions
                st.divider()
                st.subheader("🔑 Key Therapist Interventions")
                interventions = data.get("key_interventions", [])
                
                if interventions:
                    cols = st.columns(len(interventions))
                    for idx, item in enumerate(interventions):
                        with cols[idx]:
                            st.info(f"**Topic:** {item['trigger_topic']}")
                            st.write(f"**Reaction:** {item['patient_reaction']}")
                            st.caption(f"💡 {item['insight']}")
                else:
                    st.warning("No interventions found.")

                #transcript
                st.divider()
                st.subheader("📝 Transcript & Sentiment")
                
                transcript = data.get("sentence_analysis", [])
                
                for line in transcript:
                    speaker = line['speaker']
                    text = line['text']
                    emotion = line['emotion']
                    topic = line['topic']
                    
                    # Style the chat bubble based on Role
                    role = roles.get(speaker, "Unknown")
                    avatar = "🧑‍⚕️" if role == "Therapist" else "👤"
                    
                    with st.chat_message(name=role, avatar=avatar):
                        st.markdown(f"**{emotion}** | *{topic}*")
                        st.write(text)

            except Exception as e:
                st.error(f"Error loading report: {e}")