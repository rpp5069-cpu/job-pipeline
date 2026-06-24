import os, json
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth import default

try:
    # Use default credentials provided by Workload Identity Federation
    creds, project = default()
    service = build('drive', 'v3', credentials=creds)

    # Your target Drive Folder ID from GitHub Secrets
    folder = os.environ.get('GDRIVE_FOLDER_ID')

    # Search for output files
    paths_to_check = ['job_outputs/outputs', 'job_outputs', '/tmp/job_outputs/outputs']
    output_dir = next((p for p in paths_to_check if os.path.isdir(p)), None)

    if not output_dir or not folder:
        print(f"❌ Missing info. Folder: {folder}, Dir: {output_dir}")
    else:
        print(f"Scanning {output_dir} for files...")
        for fname in os.listdir(output_dir):
            if not fname.startswith('MASTER_'): continue
            fpath = os.path.join(output_dir, fname)
            
            mime = 'application/json' if fname.endswith('.json') else \
                   'text/csv' if fname.endswith('.csv') else \
                   'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

            meta = {'name': fname, 'parents': [folder]}
            media = MediaFileUpload(fpath, mimetype=mime, resumable=True)
            
            try:
                f = service.files().create(
                    body=meta, media_body=media, fields='id', supportsAllDrives=True
                ).execute()
                print(f'✅ Uploaded to Drive: {fname}')
            except Exception as e:
                print(f'❌ Drive Upload Failed for {fname}: {e}')
                if "storageQuotaExceeded" in str(e):
                    print("💡 Note: Service Account has no quota in 'My Drive'. Use a 'Shared Drive' or check GitHub Releases.")
except Exception as main_e:
    print(f"Critical Error in upload script: {main_e}")
