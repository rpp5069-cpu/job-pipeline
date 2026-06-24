import os, json
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth import default

# Use default credentials provided by Workload Identity Federation
creds, project = default()
service = build('drive', 'v3', credentials=creds)

# Your target Drive Folder ID
folder = os.environ.get('GDRIVE_FOLDER_ID', '1tU6CzQuiXUH-fbCbsy3egB94eAAT-4ZN')

# Look for outputs in relative (GHA) and absolute (Colab) paths
paths_to_check = [
    'job_outputs/outputs',
    'job_outputs',
    '/tmp/job_outputs/outputs',
    '/tmp/job_outputs'
]

output_dir = None
for p in paths_to_check:
    if os.path.isdir(p):
        output_dir = p
        break

if not output_dir:
    print(f"❌ Error: Output directory not found. Checked: {paths_to_check}")
else:
    print(f"Scanning {output_dir} for files to upload to folder {folder}...")
    for fname in os.listdir(output_dir):
        if not fname.startswith('MASTER_'):
            continue
        fpath = os.path.join(output_dir, fname)
        
        # Determine MimeType
        if fname.endswith('.json'): mime = 'application/json'
        elif fname.endswith('.csv'): mime = 'text/csv'
        else: mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

        meta = {'name': fname, 'parents': [folder]}
        media = MediaFileUpload(fpath, mimetype=mime, resumable=True)
        
        try:
            # supportsAllDrives=True is necessary for service accounts uploading to shared spaces
            f = service.files().create(
                body=meta, 
                media_body=media, 
                fields='id', 
                supportsAllDrives=True
            ).execute()
            print(f'✅ Uploaded {fname} → ID: {f.get("id")}')
        except Exception as e:
            print(f'❌ Failed to upload {fname}: {e}')
            if "storageQuotaExceeded" in str(e):
                print("💡 Tip: Service accounts have no quota. If this is 'My Drive', try using a 'Shared Drive' instead.")
