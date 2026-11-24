"""Upload this run's MASTER_* files (and collection_status.json) to Google Drive.

Credentials, in order of preference:
  1. OAuth as the Drive owner (works for a personal My Drive folder). Needs three repository secrets:
       GDRIVE_OAUTH_CLIENT_ID, GDRIVE_OAUTH_CLIENT_SECRET, GDRIVE_OAUTH_REFRESH_TOKEN
     Generate the refresh token once with scripts/get_drive_refresh_token.py.
  2. The workflow's service account (Application Default Credentials). This only works if
     GDRIVE_FOLDER_ID is a folder on a Shared Drive where the service account is a member:
     service accounts have no Drive storage of their own, so they cannot create files in a My Drive.

Exits with status 1 when anything fails, so the workflow step shows the problem.
"""
import os, sys, time
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SCOPES = ['https://www.googleapis.com/auth/drive']
MAX_RETRIES = 3
UPLOAD_PREFIXES = ('MASTER_', 'collection_status')


def get_credentials():
    """Return (credentials, mode)."""
    cid, secret, token = (os.environ.get(k, '').strip() for k in
                          ('GDRIVE_OAUTH_CLIENT_ID', 'GDRIVE_OAUTH_CLIENT_SECRET', 'GDRIVE_OAUTH_REFRESH_TOKEN'))
    if cid and secret and token:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        creds = Credentials(None, refresh_token=token, token_uri='https://oauth2.googleapis.com/token',
                            client_id=cid, client_secret=secret, scopes=SCOPES)
        creds.refresh(Request())
        return creds, 'oauth-user'
    if any((cid, secret, token)):
        raise SystemExit('❌ Only some of GDRIVE_OAUTH_CLIENT_ID / _CLIENT_SECRET / _REFRESH_TOKEN are set; set all three.')
    from google.auth import default
    creds, _ = default(scopes=SCOPES)
    return creds, 'service-account'


def check_folder(service, folder_id, mode):
    """Fail early with a clear message instead of a storageQuotaExceeded error per file."""
    meta = service.files().get(fileId=folder_id, fields='id, name, driveId, mimeType',
                               supportsAllDrives=True).execute()
    if meta.get('mimeType') != 'application/vnd.google-apps.folder':
        raise SystemExit(f'❌ GDRIVE_FOLDER_ID points to "{meta.get("name")}", which is not a folder.')
    if mode == 'service-account' and not meta.get('driveId'):
        raise SystemExit(
            f'❌ "{meta["name"]}" is in a personal My Drive. A service account cannot create files there\n'
            '   (it has no Drive storage). Set the three GDRIVE_OAUTH_* secrets to upload as the folder owner;\n'
            '   see scripts/get_drive_refresh_token.py. The data is safe in this run\'s GitHub Release.')
    return meta


def find_existing_file(service, folder_id, fname):
    q = f"name = '{fname}' and '{folder_id}' in parents and trashed = false"
    files = service.files().list(q=q, fields='files(id, name)', supportsAllDrives=True,
                                 includeItemsFromAllDrives=True).execute().get('files', [])
    return files[0]['id'] if files else None


def mime_for(fname):
    return ('application/json' if fname.endswith('.json') else 'text/csv' if fname.endswith('.csv')
            else 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


def upload_file(service, fpath, fname, folder_id):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            existing = find_existing_file(service, folder_id, fname)
            media = MediaFileUpload(fpath, mimetype=mime_for(fname), resumable=True)
            if existing:
                service.files().update(fileId=existing, media_body=media, supportsAllDrives=True).execute()
                print(f'✅ Updated  {fname}  (id={existing})')
            else:
                r = service.files().create(body={'name': fname, 'parents': [folder_id]}, media_body=media,
                                           fields='id', supportsAllDrives=True).execute()
                print(f'✅ Uploaded {fname}  (id={r["id"]})')
            return True
        except Exception as e:
            print(f'⚠️  Attempt {attempt}/{MAX_RETRIES} failed for {fname}: {e}')
            if 'storageQuotaExceeded' in str(e):
                return False
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    return False


def main():
    folder = os.environ.get('GDRIVE_FOLDER_ID', '').strip()
    output_dir = next((p for p in ('job_outputs/outputs', 'job_outputs', '/tmp/job_outputs/outputs') if os.path.isdir(p)), None)
    if not folder or not output_dir:
        raise SystemExit(f'❌ GDRIVE_FOLDER_ID: {"set" if folder else "MISSING"} | output dir: {output_dir or "NOT FOUND"}')
    creds, mode = get_credentials()
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    meta = check_folder(service, folder, mode)
    print(f'Uploading as {mode} to folder "{meta["name"]}"')
    files = sorted(f for f in os.listdir(output_dir) if f.startswith(UPLOAD_PREFIXES))
    if not files:
        raise SystemExit(f'❌ No MASTER_* files in {output_dir}')
    failed = [f for f in files if not upload_file(service, os.path.join(output_dir, f), f, folder)]
    print(f'Done: {len(files) - len(failed)} uploaded/updated, {len(failed)} failed.')
    if failed:
        raise SystemExit(f'❌ Failed: {failed}. The same files are in this run\'s GitHub Release.')


if __name__ == '__main__':
    main()
