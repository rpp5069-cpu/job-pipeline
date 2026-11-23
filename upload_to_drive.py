import os, time
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth import default

MAX_RETRIES = 3


def get_service():
    creds, _ = default()
    return build('drive', 'v3', credentials=creds)


def find_existing_file(service, folder_id, fname):
    """Return the file id if fname already exists in folder_id, else None.
    Searches both regular Drive and Shared Drives."""
    q = f"name = '{fname}' and '{folder_id}' in parents and trashed = false"
    result = service.files().list(
        q=q,
        fields='files(id, name)',
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = result.get('files', [])
    return files[0]['id'] if files else None


def upload_file(service, fpath, fname, folder_id):
    """Upload fname to folder_id, updating in place if it already exists.
    Returns True on success, False on permanent failure."""
    mime = (
        'application/json' if fname.endswith('.json') else
        'text/csv'         if fname.endswith('.csv')  else
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            existing_id = find_existing_file(service, folder_id, fname)
            media = MediaFileUpload(fpath, mimetype=mime, resumable=True)

            if existing_id:
                # Update existing file — avoids duplicates piling up each run
                service.files().update(
                    fileId=existing_id,
                    media_body=media,
                    supportsAllDrives=True,
                ).execute()
                print(f'✅ Updated in Drive : {fname}  (id={existing_id})')
            else:
                # Create new file
                meta = {'name': fname, 'parents': [folder_id]}
                result = service.files().create(
                    body=meta,
                    media_body=media,
                    fields='id',
                    supportsAllDrives=True,
                ).execute()
                print(f'✅ Uploaded to Drive: {fname}  (id={result["id"]})')

            return True

        except Exception as e:
            err = str(e)
            print(f'⚠️  Attempt {attempt}/{MAX_RETRIES} failed for {fname}: {e}')
            if 'storageQuotaExceeded' in err:
                print(
                    '💡 Service Account has no personal Drive quota.\n'
                    '   Share the target folder with the SA as Editor, or use a Shared Drive.\n'
                    '   Data is still available via the GitHub Release created in this run.'
                )
                return False   # Permanent — no point retrying
            if attempt < MAX_RETRIES:
                wait = 2 ** attempt
                print(f'   Retrying in {wait}s …')
                time.sleep(wait)

    return False


# ── Main ──────────────────────────────────────────────────────────────────────
try:
    service = get_service()
    folder  = os.environ.get('GDRIVE_FOLDER_ID')

    paths_to_check = [
        'job_outputs/outputs',
        'job_outputs',
        '/tmp/job_outputs/outputs',
    ]
    output_dir = next((p for p in paths_to_check if os.path.isdir(p)), None)

    if not output_dir or not folder:
        print(
            f'❌ Cannot proceed.\n'
            f'   GDRIVE_FOLDER_ID: {"set ✓" if folder else "MISSING ✗"}\n'
            f'   output_dir      : {output_dir or "NOT FOUND ✗"}'
        )
    else:
        print(f'Scanning {output_dir} for MASTER_* files …')
        uploaded, failed = 0, 0
        for fname in sorted(os.listdir(output_dir)):
            if not fname.startswith('MASTER_'):
                continue
            fpath = os.path.join(output_dir, fname)
            if upload_file(service, fpath, fname, folder):
                uploaded += 1
            else:
                failed += 1

        print(f'\nDone: {uploaded} uploaded/updated, {failed} failed.')
        if failed:
            print(
                '⚠️  Some files could not be uploaded to Drive — see errors above.\n'
                '   The same files are attached to the GitHub Release created in this run.'
            )

except Exception as main_e:
    print(f'Critical Error in upload script: {main_e}')
