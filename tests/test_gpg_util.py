import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from glacier_backup import gpg_util
from glacier_backup.gpg_util import GpgUtil, KEYSERVER


@pytest.fixture
def gpg_client(monkeypatch):
    client = Mock()
    monkeypatch.setattr(gpg_util, "gpg", client)
    return client


def test_get_key_normalizes_key_and_trusts_fingerprint(gpg_client, caplog):
    key_data = {"fingerprint": "FINGERPRINT", "uids": ["Backup User"]}
    gpg_client.recv_keys.return_value = SimpleNamespace(count=1)
    gpg_client.list_keys.return_value = SimpleNamespace(key_map={"ABC123": key_data})

    with caplog.at_level(logging.INFO, logger=gpg_util.__name__):
        result = GpgUtil.get_key("abc123")

    assert result == key_data
    gpg_client.recv_keys.assert_called_once_with(KEYSERVER, "ABC123")
    gpg_client.list_keys.assert_called_once_with(keys="ABC123")
    gpg_client.trust_keys.assert_called_once_with("FINGERPRINT", "TRUST_ULTIMATE")
    assert "Fingerprint of key is FINGERPRINT and uid is ['Backup User']" in caplog.text


def test_get_key_raises_when_no_keys_are_found(gpg_client):
    gpg_client.recv_keys.return_value = SimpleNamespace(count=0)

    with pytest.raises(ValueError, match="Could not get any keys for:ABC123"):
        GpgUtil.get_key("abc123")

    gpg_client.recv_keys.assert_called_once_with(KEYSERVER, "ABC123")
    gpg_client.list_keys.assert_not_called()
    gpg_client.trust_keys.assert_not_called()


def test_encrypt_file_passes_source_and_options_to_gpg(gpg_client, tmp_path, caplog):
    source_path = tmp_path / "backup.tar.gz"
    source_path.write_bytes(b"encrypted backup contents")
    gpg_client.encrypt_file.return_value = SimpleNamespace(
        ok=True, status="encryption complete", stderr=""
    )

    with caplog.at_level(logging.DEBUG, logger=gpg_util.__name__):
        result = GpgUtil.encrypt_file("FINGERPRINT", source_path, "backup.tar.gz.gpg")

    assert result is None
    gpg_client.encrypt_file.assert_called_once()
    source_file = gpg_client.encrypt_file.call_args.args[0]
    assert source_file.name == str(source_path)
    assert source_file.mode == "rb"
    assert source_file.closed
    assert gpg_client.encrypt_file.call_args.kwargs == {
        "output": "backup.tar.gz.gpg",
        "armor": False,
        "recipients": "FINGERPRINT",
    }
    assert "Encryption status: True encryption complete " in caplog.text


def test_encrypt_file_raises_when_gpg_reports_failure(gpg_client, tmp_path):
    source_path = tmp_path / "backup.tar.gz"
    source_path.write_bytes(b"backup contents")
    gpg_client.encrypt_file.return_value = SimpleNamespace(
        ok=False, status="invalid recipient", stderr="no recipient"
    )

    with pytest.raises(
        RuntimeError,
        match="Error when encrypting: no recipient, status=invalid recipient",
    ):
        GpgUtil.encrypt_file("FINGERPRINT", source_path, "backup.tar.gz.gpg")

    gpg_client.encrypt_file.assert_called_once()
    assert gpg_client.encrypt_file.call_args.kwargs["recipients"] == "FINGERPRINT"
