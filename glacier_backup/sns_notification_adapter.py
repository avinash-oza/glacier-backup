import datetime
import logging

import boto3


class NotificationAdapter:
    def __init__(self):
        self._logger = None

    def send_notification(self, message: str, log_level=logging.INFO):
        raise NotImplementedError("Subclasses must implement this method")

    def set_logger(self, logger):
        self._logger = logger

    def _log(self, message: str, log_level=logging.INFO):
        if self._logger:
            self._logger.log(log_level, message)


class NoNotificationAdapter(NotificationAdapter):
    def send_notification(self, message: str, log_level=logging.INFO):
        self._log(message, log_level)


class SnsNotificationAdapter(NotificationAdapter):
    def __init__(self, topic_arn, assume_role_arn=None):
        self._assumed_role_arn = assume_role_arn
        self._topic_arn = topic_arn

        self.__sns_client = None
        self.__assumed_role_credentials_expiry = datetime.datetime.min.replace(
            tzinfo=datetime.timezone.utc
        )

        super().__init__()

    def send_notification(self, message: str, log_level=logging.INFO):
        self._log(message, log_level)

        self._sns_client.publish(
            TopicArn=self._topic_arn,
            Message=message,
        )

    @property
    def _sns_client(self):
        if (
            self.__sns_client is None
            or datetime.datetime.now(datetime.timezone.utc)
            >= self.__assumed_role_credentials_expiry
        ):
            if self._assumed_role_arn:
                self._log(
                    "Assuming role to get temporary credentials for SNS client",
                    logging.INFO,
                )
                credentials = self._get_assumed_role_credentials(self._assumed_role_arn)
                self.__sns_client = boto3.client(
                    "sns",
                    aws_access_key_id=credentials["AccessKeyId"],
                    aws_secret_access_key=credentials["SecretAccessKey"],
                    aws_session_token=credentials["SessionToken"],
                )
                # leave some buffer in the expiry time before renewing the credentials
                self.__assumed_role_credentials_expiry = credentials[
                    "Expiration"
                ] - datetime.timedelta(minutes=20)
                self._log(
                    f"Assumed role credentials will expire at {self.__assumed_role_credentials_expiry.isoformat()}",
                    logging.INFO,
                )
            else:
                # nothing to follow regarding time, so we can set the expiry to max value
                self.__sns_client = boto3.client("sns")
                self.__assumed_role_credentials_expiry = datetime.datetime.max.replace(
                    tzinfo=datetime.timezone.utc
                )
        return self.__sns_client

    @staticmethod
    def _get_assumed_role_credentials(role_arn):
        sts_client = boto3.client("sts")
        assumed_role_object = sts_client.assume_role(
            RoleArn=role_arn, RoleSessionName="GlacierBackupScriptSession"
        )
        return assumed_role_object["Credentials"]
