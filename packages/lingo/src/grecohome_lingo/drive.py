"""Google Drive access for Lingo — service-account auth, list + download.

Auth is a service account (the Drive folder is shared read-only with the SA's
email), so there's no interactive OAuth and no token refresh. Files are
downloaded to **bytes in memory** (no temp file) and handed straight to bronze.
"""

import io

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from grecohome_core.logging_config import get_logger
from grecohome_lingo.config import settings

log = get_logger(__name__)

# Read-only is all we need; the SA can't modify the folder.
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def get_drive_service(service_account_path: str | None = None):
    """Build a Drive v3 client from the mounted service-account key."""
    path = service_account_path or settings.gdrive_service_account_path
    creds = service_account.Credentials.from_service_account_file(path, scopes=SCOPES)
    # cache_discovery=False avoids a noisy file-cache warning on some setups.
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def list_csv_files(service, folder_id: str | None = None) -> list[dict]:
    """List the CSV files in the watched folder (id, name, created/modified times).

    Paginates so a large/long-lived folder is fully enumerated.
    """
    folder_id = folder_id or settings.gdrive_folder_id
    query = f"'{folder_id}' in parents and mimeType='text/csv' and trashed=false"
    files: list[dict] = []
    page_token = None
    while True:
        resp = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, createdTime, modifiedTime)",
                orderBy="createdTime",
                pageToken=page_token,
            )
            .execute()
        )
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files


def download_file_bytes(service, file_id: str) -> bytes:
    """Download a Drive file's content to bytes (in memory)."""
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _status, done = downloader.next_chunk()
    return buffer.getvalue()
