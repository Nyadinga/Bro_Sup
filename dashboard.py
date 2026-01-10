import streamlit as st
import time
import os
import math
import datetime
import pandas as pd
from client import client_cli as client

# Configuration
GATEWAY = "localhost:50051"
FREE_TIER_LIMIT = 2 * 1024 * 1024 * 1024  # 2 GB

# CAMEROON REGION COORDINATES (Lat, Lon)
REGION_COORDS = {
    "Yaoundé": [3.8480, 11.5021],
    "Douala": [4.0511, 9.7679],
    "Buea": [4.1550, 9.2435],
    "Bamenda": [5.9631, 10.1591],
    "Garoua": [9.3014, 13.3977],
    "Maroua": [10.5930, 14.3208],
    "Bafoussam": [5.4778, 10.4176]
}

st.set_page_config(page_title="Bluetap Command Center", page_icon="💧", layout="wide")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .reportview-container { background: #f0f8ff }
    h1 { color: #0077b6; }
    .stButton>button { width: 100%; border-radius: 5px; }
    div[data-testid="stMetricValue"] { font-size: 1.2rem; }
    .status-critical { color: red; font-weight: bold; }
    .chunk-box {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 10px;
        background: #ffffff;
        text-align: center;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.05);
    }
</style>
""", unsafe_allow_html=True)

# --- HELPERS ---
def format_size(size_bytes):
    if size_bytes == 0: return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])

def render_sidebar():
    with st.sidebar:
        try:
            st.image("cloud.png", width=80)
        except:
            st.markdown("<h1 style='font-size: 50px; margin: 0;'>☁️</h1>", unsafe_allow_html=True)
            
        st.title("SupBro 😎")
        
        if st.session_state.get('token'):
            st.write(f"User: **{st.session_state.get('login_user')}**")
            st.success(f"System Status: ● Online")
            
            st.subheader(" Storage Quota")
            try:
                files = client.list_files(GATEWAY)
                total_used = sum(f.filesize for f in files) if files else 0
            except:
                total_used = 0
                
            usage_ratio = min(1.0, total_used / FREE_TIER_LIMIT)
            st.progress(usage_ratio)
            st.caption(f"**{format_size(total_used)}** used of **2 GB**")

            st.markdown("---")
            if st.button("Logout"):
                st.session_state.clear()
                st.rerun()

# --- NEW VISUALIZATION: CHUNK MAP ---
def display_chunk_map(file_obj):
    st.write(f"###  Distributed Topology: {file_obj.filename}")
    
    # Calculate simulated chunks (GFS usually uses 64MB chunks, we'll show up to 4 for visual)
    num_chunks = min(4, max(1, math.ceil(file_obj.filesize / (1024*1024))))
    
    cols = st.columns(num_chunks)
    for i in range(num_chunks):
        with cols[i]:
            st.markdown(f"""
            <div class="chunk-box">
                <small>Chunk ID {i}</small><br>
                <span style='color:blue'>Node A</span> <br>
                <span style='color:blue'>Node B</span> 
            </div>
            """, unsafe_allow_html=True)

# --- APP START ---
if 'token' not in st.session_state: st.session_state['token'] = None
if 'otp_sent' not in st.session_state: st.session_state['otp_sent'] = False

render_sidebar()

if not st.session_state['token']:
    # [Login Logic remains as you had it]
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.title("SupBro Login")
        if not st.session_state['otp_sent']:
            with st.form("login"):
                user = st.text_input("Username")
                email = st.text_input("Email")
                if st.form_submit_button("Request Access Code"):
                    ok, msg = client.login(GATEWAY, user, email)
                    if ok:
                        st.session_state['otp_sent'] = True
                        st.session_state['login_user'] = user
                        st.session_state['login_email'] = email
                        st.rerun()
                    else: st.error(msg)
        else:
            otp = st.text_input("OTP Code", type="password")
            if st.button("Verify"):
                ok, token = client.verify_otp_and_get_token(GATEWAY, st.session_state['login_user'], otp)
                if ok:
                    st.session_state['token'] = token
                    st.rerun()
else:
    client.set_token(st.session_state['token'])
    files = client.list_files(GATEWAY)
    
    # KPI Metrics
    k1, k2, k3 = st.columns(3)
    k1.metric("Infrastructure", "RAID-1 Mirror", "Healthy")
    k2.metric("Files Managed", len(files))
    k3.metric("Replication Factor", "2x")

    tab1, tab2 = st.tabs(["🚀 Upload & Sync", "📁 Data Gallery"])

    with tab1:
        uploaded_file = st.file_uploader("Upload Infrastructure Report")
        if uploaded_file and st.button("Submit to Grid", type="primary"):
            temp_path = f"temp_{uploaded_file.name}"
            with open(temp_path, "wb") as f: f.write(uploaded_file.getbuffer())
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def update_ui(chunk_id, fname, node):
                status_text.text(f"Streaming chunk {chunk_id} to {node}...")
                progress_bar.progress(min(100, (chunk_id + 1) * 20))

            ok, msg = client.put_file(GATEWAY, temp_path, progress_callback=update_ui)
            if ok:
                st.success("Replication Complete!")
                os.remove(temp_path)
                st.rerun()
            else: st.error(msg)

    with tab2:
        if not files:
            st.info("No files found in your storage.")
        else:
            # 1. Group files by folder (Assumes f has a folder_name attribute)
            # If your proto doesn't have folder_name yet, we fallback to 'Root'
            grouped_files = {}
            for f in files:
                # Use getattr to prevent crashing if folder_name isn't in your current proto
                folder = getattr(f, 'folder_name', 'Default Storage') 
                if folder not in grouped_files:
                    grouped_files[folder] = []
                grouped_files[folder].append(f)

            # 2. Render Folders
            for folder_name, folder_files in grouped_files.items():
                st.markdown(f"### 📁 {folder_name}")
                
                # Render each file inside this folder
                for f in folder_files:
                    with st.expander(f"📄 {f.filename} ({format_size(f.filesize)})"):
                        display_chunk_map(f) # Show the chunks topology!
                        
                        c1, c2 = st.columns([8, 2])
                        c1.write(f"**Uploaded:** {f.created_at}")
                        
                        # Retrieval Logic
                        ready_key = f"ready_{f.upload_id}"
                        local_path = f"downloaded_{f.filename}"
                        
                        if st.session_state.get(ready_key) and os.path.exists(local_path):
                            with open(local_path, "rb") as fh:
                                c2.download_button("💾 Open", fh, file_name=f.filename, key=f"dl_{f.upload_id}")
                        else:
                            if c2.button("Retrieve", key=f"fetch_{f.upload_id}"):
                                with st.spinner("Streaming from nodes..."):
                                    ok, msg = client.download_file(GATEWAY, f.filename, local_path)
                                    if ok:
                                        st.session_state[ready_key] = True
                                        st.rerun()
                                    else:
                                        st.error(msg)
                st.markdown("---") # Visual separator between folders