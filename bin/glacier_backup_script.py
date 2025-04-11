import logging
import os

import click

from glacier_backup.backup_runner import BackupRunner

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s.%(funcName)s %(message)s",
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--gpg-key-id", help="Fingerprint of the key to use")
@click.option("--input-file-path", help="Local file containing directory list")
@click.option("--temp-dir", help="dir to use for scratch space")
def create_archives(gpg_key_id, input_file_path, temp_dir):
    if not os.path.exists(temp_dir):
        raise ValueError(f"temp dir does not exist, create before running")

    backup_runner = BackupRunner(temp_dir=temp_dir)

    backup_runner.run(input_file_path, gpg_key_id)


if __name__ == "__main__":

    cli()
