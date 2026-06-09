"""Tests for the Google Drive client (mocked googleapiclient)."""

from unittest.mock import MagicMock, patch

import pytest
from grecohome_lingo import drive


@pytest.mark.unit
class TestGetDriveService:
    def test_builds_with_service_account_creds(self):
        with (
            patch("grecohome_lingo.drive.service_account") as sa,
            patch("grecohome_lingo.drive.build") as build,
        ):
            sa.Credentials.from_service_account_file.return_value = "creds"
            build.return_value = "svc"
            svc = drive.get_drive_service("/secrets/lingo/sa.json")
            assert svc == "svc"
            sa.Credentials.from_service_account_file.assert_called_once_with(
                "/secrets/lingo/sa.json", scopes=drive.SCOPES
            )
            build.assert_called_once_with("drive", "v3", credentials="creds", cache_discovery=False)


@pytest.mark.unit
class TestListCsvFiles:
    def test_single_page(self):
        service = MagicMock()
        service.files.return_value.list.return_value.execute.return_value = {
            "files": [{"id": "f1", "name": "g.csv"}],
            "nextPageToken": None,
        }
        files = drive.list_csv_files(service, folder_id="folder1")
        assert files == [{"id": "f1", "name": "g.csv"}]

    def test_paginates(self):
        service = MagicMock()
        service.files.return_value.list.return_value.execute.side_effect = [
            {"files": [{"id": "f1"}], "nextPageToken": "tok"},
            {"files": [{"id": "f2"}], "nextPageToken": None},
        ]
        files = drive.list_csv_files(service, folder_id="folder1")
        assert [f["id"] for f in files] == ["f1", "f2"]


@pytest.mark.unit
class TestDownloadFileBytes:
    def test_downloads_to_bytes(self):
        class _FakeDownloader:
            def __init__(self, fh, _request):
                self._fh = fh

            def next_chunk(self):
                self._fh.write(b"glucose-bytes")
                return (None, True)  # done on first chunk

        service = MagicMock()
        with patch("grecohome_lingo.drive.MediaIoBaseDownload", _FakeDownloader):
            data = drive.download_file_bytes(service, "f1")
        assert data == b"glucose-bytes"
        service.files.return_value.get_media.assert_called_once_with(fileId="f1")
