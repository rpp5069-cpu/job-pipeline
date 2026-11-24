"""One-time setup: create the OAuth refresh token the weekly workflow uses to upload to your Drive.

Run this on your own computer (it opens a browser to sign in), not in GitHub Actions.

1. Google Cloud console, project used by the pipeline -> APIs & Services:
   a. Library: make sure "Google Drive API" is enabled.
   b. OAuth consent screen: User type "External"; add yourself as a test user; then click
      "Publish app" so the status is "In production". (While it is "Testing", Google expires
      refresh tokens after 7 days and the weekly upload would start failing.)
   c. Credentials -> Create credentials -> OAuth client ID -> Application type "Desktop app".
      Download the JSON file.
2. pip install google-auth-oauthlib
3. python scripts/get_drive_refresh_token.py path/to/client_secret.json
   Sign in as the account that owns the Drive folder (the "unverified app" warning is expected
   for your own app: Advanced -> Go to app).
4. Add the three printed values as repository secrets (Settings -> Secrets and variables -> Actions):
   GDRIVE_OAUTH_CLIENT_ID, GDRIVE_OAUTH_CLIENT_SECRET, GDRIVE_OAUTH_REFRESH_TOKEN
Never commit the client JSON or the token.
"""
import json, sys
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/drive']

if len(sys.argv) != 2:
    sys.exit('usage: python scripts/get_drive_refresh_token.py client_secret.json')
flow = InstalledAppFlow.from_client_secrets_file(sys.argv[1], SCOPES)
creds = flow.run_local_server(port=0, access_type='offline', prompt='consent')
cfg = json.load(open(sys.argv[1]))
cfg = cfg.get('installed') or cfg.get('web')
print('\nAdd these three repository secrets:\n')
print('GDRIVE_OAUTH_CLIENT_ID     =', cfg['client_id'])
print('GDRIVE_OAUTH_CLIENT_SECRET =', cfg['client_secret'])
print('GDRIVE_OAUTH_REFRESH_TOKEN =', creds.refresh_token)
