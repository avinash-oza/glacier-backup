import argparse
import logging
import os

from glacier_backup.backup_runner import BackupRunner

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s %(name)s.%(funcName)s %(message)s",
    )
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--gpg-key-id", type=str, required=True, help="Fingerprint of the key to use"
    )
    parser.add_argument(
        "--input-file-path",
        type=str,
        required=True,
        help="Local file containing directory list",
    )
    parser.add_argument("--temp-dir", type=str, help="dir to use for scratch space")
    parser.add_argument(
        "--apprise-config-path",
        required=False,
        type=str,
        help="path to apprise config file to use",
    )

    args = parser.parse_args()
    temp_dir = args.temp_dir
    input_path = args.input_file_path
    apprise_config_path = args.apprise_config_path
    enable_apprise = apprise_config_path is not ""

    if not os.path.exists(temp_dir):
        raise ValueError(f"temp dir does not exist, create before running")

    apprise_obj = None
    if enable_apprise:
        logger.info(f"Enabling apprise for notifications")
        import apprise

        apprise_obj = apprise.Apprise()
        config = apprise.AppriseConfig()

        config.add(apprise_config_path)
        apprise_obj.add(config)
        apprise_obj.notify(title="", body="Configured apprise successfully")

    g = BackupRunner(temp_dir=temp_dir, apprise_obj=apprise_obj)

    g.run(input_path, args.gpg_key_id)
