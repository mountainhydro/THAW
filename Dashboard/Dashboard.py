# -*- coding: utf-8 -*-

"""
THAW - Streamlit Dashboard landing page

Dr. Stefan Fugger

Created in Feb 2026
"""


import streamlit as st
import ee
import os

# 1. Directory Setup
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
TEMP_DIR = os.path.join(PARENT_DIR, "temp")
CRED_FILE = os.path.join(TEMP_DIR, "gee_credentials.txt")

if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)

# 2. Page Config
st.set_page_config(page_title="Welcome!", layout="centered")

# 3. Helper Functions
def save_creds(project, path):
    with open(CRED_FILE, "w") as f:
        f.write(f"{project}\n{path}")

def load_creds():
    if os.path.exists(CRED_FILE):
        with open(CRED_FILE, "r") as f:
            lines = [line.strip() for line in f.readlines()]
            if len(lines) >= 2:
                return lines[0], lines[1]
    return "", ""

def delete_creds():
    """ONLY called when Logout is pressed."""
    if os.path.exists(CRED_FILE):
        os.remove(CRED_FILE)

def init_gee(project):
    try:
        ee.Initialize(project=project)
        return True
    except Exception:
        return False

# 4. Persistent Session Check
saved_project, saved_path = load_creds()

# If session is fresh, try to auto-login from the file
if "authenticated" not in st.session_state:
    if saved_project:
        if init_gee(saved_project):
            st.session_state.authenticated = True
            st.session_state.active_project = saved_project
            st.session_state.active_path = saved_path
        else:
            st.session_state.authenticated = False
    else:
        st.session_state.authenticated = False

# 5. UI Logic
if not st.session_state.authenticated:
    st.markdown("<style>section[data-testid='stSidebar'] {display: none;}</style>", unsafe_allow_html=True)
    st.title("Sentinel-1 SAR Water Monitor (THAW)")
    
    with st.container(border=True):
        project_input = st.text_input("GEE Project ID", value=saved_project)
        path_input = st.text_input("Service Account JSON Path", value=saved_path, placeholder="C:\\...\\key.json")
        
        if st.button("Login & Remember Me", use_container_width=True):
            if project_input:
                ee.Authenticate() 
                if init_gee(project_input):
                    # Writing to file ensures it persists across restarts
                    save_creds(project_input, path_input)
                    st.session_state.authenticated = True
                    st.session_state.active_project = project_input
                    st.session_state.active_path = path_input
                    st.rerun()
                else:
                    st.error("Initialization failed. Check your Project ID.")
            else:
                st.warning("Project ID is required.")

    # --- GUIDE SECTION ---
    st.divider()
    st.subheader("📖 How to get your Credentials")
    with st.expander("Step 1: Get a Google Earth Engine Account"):
        st.write("Sign up for access at [earthengine.google.com](https://earthengine.google.com/signup).")

    with st.expander("Step 2: Create & Initialize your Project"):
        st.markdown("""
        1. **Create project:** [GEE Cloud Console](https://console.cloud.google.com/earth-engine/).
        2. **Enable API:** Visit the [API Library](https://console.cloud.google.com/apis/library/earthengine.googleapis.com) and click **Enable**.
        """)
    
    with st.expander("Step 3: Create a Service Account"):
        st.markdown("""
        1. Go to [IAM & Admin > Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts).
        2. Create a key (JSON). Save the path.
        3. Share your `GEE_Exports` Drive folder with the Service Account email.
        """)

else:
    # --- MAIN APP PAGE ---
    st.sidebar.title("Navigation")

    if st.sidebar.button("Logout"):
        delete_creds()
        for key in ["authenticated", "active_project", "active_path"]:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
    
    st.title("Welcome to THAW")
    
    st.success(
        f"### Session Active\n\n"
        f"**Project ID:** `{st.session_state.get('active_project')}`\n\n"
        f"**Service Account path:** `{st.session_state.get('active_path')}`"
    )
    
    try:
        status = ee.String("Earth Engine Engine Online").getInfo()
        st.info(f"**Status:** {status}")
    except Exception as e:
        st.error(f"Could not reach GEE servers: {e}")

    st.divider()
    st.write("You are fully authenticated. The application will remember you until you click Logout.")