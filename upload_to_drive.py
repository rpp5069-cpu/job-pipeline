import os, json
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth import default # Import default credentials

# Use default credentials provided by Workload Identity Federation
creds, project = default()
service = build('drive', 'v3', credentials=creds)
folder = os.environ['GDRIVE_FOLDER_ID'] # This secret is still needed

output_dir = '/tmp/job_outputs/outputs'
if not os.path.isdir(output_dir): # Corrected: os.path() was incorrect
    output_dir = '/tmp/job_outputs'

for fname in os.listdir(output_dir):
    if not fname.startswith('MASTER_'):
        continue
    fpath = os.path.join(output_dir, fname)
    mime = ('application/json' if fname.endswith('.json') else
            'text/csv' if fname.endswith('.csv') else
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    meta = {'name': fname, 'parents': [folder]}
    media = MediaFileUpload(fpath, mimetype=mime, resumable=True)
    f = service.files().create(body=meta, media_body=media, fields='id').execute()
    print(f'Uploaded {fname} → Drive ID {f.get("id")}')
