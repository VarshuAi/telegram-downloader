"""
Run this script ONCE on your local machine to authorize Google Drive access.
It will print a REFRESH_TOKEN and CLIENT_ID and CLIENT_SECRET to store in GitHub Secrets.

Steps:
1. Go to https://console.cloud.google.com
2. Create a project (or reuse existing one)
3. Enable Google Drive API
4. Go to APIs & Services -> Credentials -> Create Credentials -> OAuth 2.0 Client ID
5. Set Application type = Desktop App, name it anything
6. Download the client secret JSON file
7. Run: python generate_gdrive_token.py

Usage:
    pip install google-auth-oauthlib google-api-python-client
    python generate_gdrive_token.py
"""
import json
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/drive']

client_secrets_path = input("Enter the path to your downloaded OAuth client_secret JSON file: ").strip().strip('"')

flow = InstalledAppFlow.from_client_secrets_file(client_secrets_path, SCOPES)

# Opens your browser for Google login
creds = flow.run_local_server(port=0)

with open(client_secrets_path) as f:
    client_data = json.load(f)
    
client_info = client_data.get('installed') or client_data.get('web')

print("\n" + "="*70)
print("SUCCESS! Add these 3 values as GitHub Repository Secrets:")
print("="*70)
print(f"GDRIVE_CLIENT_ID     = {client_info['client_id']}")
print(f"GDRIVE_CLIENT_SECRET = {client_info['client_secret']}")
print(f"GDRIVE_REFRESH_TOKEN = {creds.refresh_token}")
print("="*70)
print("\nGo to: GitHub Repo -> Settings -> Secrets and variables -> Actions")
print("Add each of the above 3 as separate secrets.")
print("Also DELETE the old GDRIVE_SERVICE_ACCOUNT_JSON secret - it's no longer needed.")
