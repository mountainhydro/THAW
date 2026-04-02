# -*- coding: utf-8 -*-
"""
THAW - Streamlit Dashboard entry point

Dr. Stefan Fugger
"""
import os
import streamlit as st
import ee

# 1. Paths
TEMP_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "temp"))
DRIVE_TOKEN_FILE = os.path.join(TEMP_DIR, "drive_token.json")
CRED_FILE = os.path.join(TEMP_DIR, "gee_credentials.txt")

os.makedirs(TEMP_DIR, exist_ok=True)

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
        path_input = st.text_input("OAuth Client Secret JSON Path (please do not enter any quotes))", value=saved_path, placeholder="C:\\...\\client_secret.json")

        if st.button("Login & Remember Me", use_container_width=True):
            if project_input:
                if init_gee(project_input):
                    # Run Drive OAuth flow (always on first login, token saved to ROOT/temp/)
                    if path_input and os.path.exists(path_input):
                        try:
                            from google_auth_oauthlib.flow import InstalledAppFlow
                            flow = InstalledAppFlow.from_client_secrets_file(
                                path_input,
                                scopes=["https://www.googleapis.com/auth/drive"],
                            )
                            creds = flow.run_local_server(port=0)
                            with open(DRIVE_TOKEN_FILE, "w") as tf:
                                tf.write(creds.to_json())
                            st.success(f"Drive token saved to: {DRIVE_TOKEN_FILE}")
                        except Exception as e:
                            st.error(f"Drive authorisation failed: {e}")
                            st.stop()
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

    with st.expander("Step 3: Create an OAuth 2.0 Client Secret"):
        st.markdown("""
        1. Go to [APIs & Services > Credentials](https://console.cloud.google.com/apis/credentials).
        2. Click **Create Credentials** → **OAuth 2.0 Client ID**.
        3. Application type: **Desktop app** → Create.
        4. Download the JSON and paste its path above.

        A browser window will open once during login to authorise Drive access.
        The token is saved automatically and reused on all subsequent runs.
        """)

else:
    # --- MAIN APP PAGE ---
    st.sidebar.title("Navigation")

    if st.sidebar.button("Logout"):
        delete_creds()
        if os.path.exists(DRIVE_TOKEN_FILE):
            os.remove(DRIVE_TOKEN_FILE)
        for key in ["authenticated", "active_project", "active_path"]:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

    st.title("Welcome to THAW")



    try:
        status = ee.String("Earth Engine Online").getInfo()
        gee_ok = True
    except Exception as e:
        status = f"Unreachable ({e})"
        gee_ok = False

    drive_ok = os.path.exists(DRIVE_TOKEN_FILE)

    st.success(
        f"### Session Active\n\n"
        f"**Project ID:** `{st.session_state.get('active_project')}`\n\n"
        f"**Google Earth Engine:** {status}\n\n"
        f"**Google Drive:** {'Authorised' if drive_ok else 'Not authorised ✗'}"
    )

    if gee_ok and drive_ok:
        st.write("You are fully authenticated. The application will remember you until you click Logout.")
    else:
        st.warning("Some services are not yet authorised. Check the status above.")

    # Run Drive OAuth if token is missing
    if not os.path.exists(DRIVE_TOKEN_FILE):
        client_secret_path = st.session_state.get("active_path", "")
        if client_secret_path and os.path.exists(client_secret_path):
            st.info("Google Drive authorisation required. A browser window will open...")
            try:
                from google_auth_oauthlib.flow import InstalledAppFlow
                flow = InstalledAppFlow.from_client_secrets_file(
                    client_secret_path,
                    scopes=["https://www.googleapis.com/auth/drive"],
                )
                creds = flow.run_local_server(port=0)
                with open(DRIVE_TOKEN_FILE, "w") as tf:
                    tf.write(creds.to_json())
                st.success(f"Drive token saved to: {DRIVE_TOKEN_FILE}")
            except Exception as e:
                st.error(f"Drive authorisation failed: {e}")
        else:
            st.warning("OAuth Client Secret path not found. Please log out and log in again with a valid path.")
    else:
        pass
