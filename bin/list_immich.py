import argparse
import csv
import datetime
import logging
import os

from glacier_backup.file_data import UPLOAD_TIME_EVERY_BACKUP


def generate_output_file(immich_file_root, output_file_path, full_backup):
    # thumbs -> complete directory every time
    # upload -> complete directory every time
    # library/1e958228-47fc-463d-83c7-0bc485a8cbfa/2024
    output_list = []
    output_list.extend(
        [
            (
                os.path.join(immich_file_root, "photos", "thumbs"),
                UPLOAD_TIME_EVERY_BACKUP,
            ),
            (
                os.path.join(immich_file_root, "photos", "upload"),
                UPLOAD_TIME_EVERY_BACKUP,
            ),
        ]
    )

    current_year = str(datetime.datetime.now().year)

    photos_file_path = os.path.join(immich_file_root, "photos", "library")
    for user in os.listdir(photos_file_path):
        user_file_path = os.path.join(photos_file_path, user)

        if not os.path.isdir(user_file_path):
            logger.warning(f"Skipping {user_file_path=}")
            continue

        logger.info("Username: %s", user)

        user_file_path = os.path.join(photos_file_path, user)
        for year in os.listdir(user_file_path):
            year_file_path = os.path.join(user_file_path, year)

            archive_output_file_name = f"{user}__{year}"

            if full_backup:
                output_list.append(
                    (year_file_path, UPLOAD_TIME_EVERY_BACKUP, archive_output_file_name)
                )
                continue

            if year == current_year:
                output_list.append(
                    (year_file_path, UPLOAD_TIME_EVERY_BACKUP, archive_output_file_name)
                )
                logger.info("Setting current year to glacier")
                continue
            output_list.append(
                (year_file_path, UPLOAD_TIME_EVERY_BACKUP, archive_output_file_name)
            )

    with open(output_file_path, "w") as f:
        writer = csv.writer(f, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["file_path", "upload_time", "output_file_path"])
        for r in output_list:
            writer.writerow(r)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s %(name)s.%(funcName)s %(message)s",
    )
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--immich-file-root",
    type=str,
    required=True,
    help="Root of immich file installation",
)
parser.add_argument(
    "--output-file-path",
    type=str,
    required=True,
    help="Path to write the output file",
)
parser.add_argument(
    "--full",
    action="store_true",
    help="Do a full backup (vs current year only)",
)

args = parser.parse_args()

generate_output_file(args.immich_file_root, args.output_file_path, args.full)
