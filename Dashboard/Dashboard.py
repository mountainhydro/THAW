# -*- coding: utf-8 -*-
"""
THAW - Streamlit Dashboard entry point

Dr. Stefan Fugger
"""
import os
import base64
import streamlit as st
import ee

# 1. Paths
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) 
ROOT_DIR = os.path.dirname(CURRENT_DIR)  
TEMP_DIR = os.path.normpath(os.path.join(ROOT_DIR, "temp"))
DOCS_DIR = os.path.normpath(os.path.join(ROOT_DIR, "docs"))
DRIVE_TOKEN_FILE = os.path.join(TEMP_DIR, "drive_token.json")
CRED_FILE = os.path.join(TEMP_DIR, "gee_credentials.txt")
os.makedirs(TEMP_DIR, exist_ok=True)

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/earthengine",
]

# 2. Page Config
st.set_page_config(page_title="Welcome to THAW!", layout="centered")


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

def run_oauth_flow(client_secret_path):
    """Run the OAuth flow and save the token. Returns credentials on success."""
    from google_auth_oauthlib.flow import InstalledAppFlow
    flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
    creds = flow.run_local_server(port=0)
    with open(DRIVE_TOKEN_FILE, "w") as f:
        f.write(creds.to_json())
    return creds

def load_token_credentials():
    """Load saved OAuth token and refresh if needed. Returns credentials or None."""
    if not os.path.exists(DRIVE_TOKEN_FILE):
        return None
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    creds = Credentials.from_authorized_user_file(DRIVE_TOKEN_FILE, SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(DRIVE_TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return creds

def init_gee(project):
    """Initialise GEE using the saved OAuth token if available, else default auth."""
    try:
        creds = load_token_credentials()
        if creds:
            ee.Initialize(credentials=creds, project=project)
        else:
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
                if path_input and os.path.exists(path_input):
                    try:
                        run_oauth_flow(path_input)
                    except Exception as e:
                        st.error(f"Authorisation failed: {e}")
                        st.stop()
                if init_gee(project_input):
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
    st.subheader("📖 How to set up your credentials (step by step)")

    with st.expander("Step 1: Create a Google account and sign up for Earth Engine"):
        st.markdown("""
        To use THAW you need a Google account and access to Earth Engine.
        > In case yo you already have an Earth Engine Account, you can skip directly to Step 2.
        1. If you do not have a Google account yet, create one for free at [accounts.google.com](https://accounts.google.com/).
        2. Go to [earthengine.google.com](https://earthengine.google.com/) and click **Get Started**.
        3. Log in with your Google account.
        4. Agree to terms of Service and click **Agree and Continue**.
        
        """)

    with st.expander("Step 2: Create an Earth Engine project"):
        st.markdown("""
        In order use THAW, you need to create a Google Cloud project. The previous step should have taken you there, otherwise: https://console.cloud.google.com/.

        1. In the Google Cloud Console, click **Create project**  
            - Type a short name for your project, for example: `THAW`.
            - Just below the name you will see an **ID** that Google generates automatically,
            for example `thaw-123456`. 
            - Copy this ID, and **paste it in the "GEE Project ID" field** above. 
            - If you have an organization attached to your Google Account, select it, otherwise select *no organization* and click **Create**.
        2. Select **See if you are eligible for noncommercial use** and click **Get Started**, and fill the form.
            - *Being from a public institution or research institute, you should be eligible.*       
            - The *Community Plan* should give you enough resources to run THAW for free.        
        3. If you are prompted, enable **Google Earth Engine API**
        4. Do also enable **Google Drive API** here: https://console.cloud.google.com/apis/. Search for "Google Drive API", click on it, and then click **Enable**.
                    
        > In case you cannot find your GEE Project ID or for an alternative way to create a new project, try your luck here: https://code.earthengine.google.com/
        """)

    with st.expander("Step 3: Set up the authorisation screen (done only once)"):
        st.markdown("""
        Before THAW can ask for your permission to use Earth Engine,
        Google needs a short description of the application.
        This is called the **consent screen** and you only set it up once.

        1. Open this link: [OAuth consent screen](https://console.cloud.google.com/apis/credentials/consent).
           Make sure your project name is shown in the bar at the very top of the page. Click **Get Started**
        3. Fill in the form:
           - **App name** → type `THAW`
           - **User support email** → select your own email address
           - **Audience** → Select *Internal*
           - **Contact Information** → type your email address again
        4. Continue, agree to the terms, and click **Create**.
        """)

    with st.expander("Step 4: Download your secret key file"):
        st.markdown("""
        Now you need download your API credentials in a **client secret** file.

        1. Open this link: [Credentials page](https://console.cloud.google.com/apis/credentials).
           Again, make sure your project is shown at the top.
        2. Click **+ Create Credentials** → **OAuth client ID**.
            - Under **Application type**, choose **Desktop app**.
            - Give it any name, for example `THAW`, and click **Create**.
            - A small window appears — click **Download JSON**.
            - A file will be downloaded to your computer (it has a long name starting with `client_secret_...`).
            - Move it to a safe place, for example your Documents folder.
           **Do not share this file with anyone.**
        3. Copy the full path to that file (for example `C:\\Users\\YourName\\Documents\\client_secret.json`)
           and paste it into the **OAuth Client Secret JSON Path** field at the top of this page.
           Do not include any quotation marks (`"`).
        4. Now click the **Login & Remember Me** button at the top of this page.
                    
        > **What happens when you log in for the first time:**
        > A browser window will open and ask you to confirm that THAW may access Earth Engine.
        > Select your google account and click **Allow**, THAW will remember you automatically.
        > You can close the browser window. You will not be asked again until you click the **Logout** button.
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
        f"**Google Drive:** {'Authorised ✓' if drive_ok else 'Not authorised ✗'}"
    )

    if gee_ok and drive_ok:
        st.write("You are fully authenticated. The application will remember you until you click Logout.")
    else:
        st.warning("Some services are not yet authorised. Check the status above.")

    # Run OAuth flow if token is missing
    if not os.path.exists(DRIVE_TOKEN_FILE):
        client_secret_path = st.session_state.get("active_path", "")
        if client_secret_path and os.path.exists(client_secret_path):
            st.info("Authorisation required. A browser window will open...")
            try:
                run_oauth_flow(client_secret_path)
                st.rerun()
            except Exception as e:
                st.error(f"Authorisation failed: {e}")
        else:
            st.warning("OAuth Client Secret path not found. Please log out and log in again with a valid path.")
    else:
        pass
