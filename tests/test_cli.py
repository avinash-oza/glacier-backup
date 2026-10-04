from unittest.mock import Mock

from click.testing import CliRunner
import pytest

from glacier_backup import cli as cli_module
from glacier_backup.gpg_util import KEYSERVER


@pytest.mark.parametrize(
    ("options", "expected_keyserver"),
    [
        ([], KEYSERVER),
        (["--keyserver", "hkps://keys.example"], "hkps://keys.example"),
    ],
)
def test_update_gpg_expiry_command_invokes_gpg_util(
    monkeypatch, options, expected_keyserver
):
    update_key_expiry = Mock()
    monkeypatch.setattr(cli_module.GpgUtil, "update_key_expiry", update_key_expiry)

    result = CliRunner().invoke(
        cli_module.cli, ["update-gpg-expiry", "ABC123", *options]
    )

    assert result.exit_code == 0
    update_key_expiry.assert_called_once_with("ABC123", expected_keyserver)
    assert f"uploaded ABC123 to {expected_keyserver}" in result.output
