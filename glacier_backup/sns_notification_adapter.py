import logging

import boto3


class NotificationAdapter:
    def __init__(self):
        self._logger = None

    def send_notification(self, message: str, log_level=logging.INFO):
        raise NotImplementedError("Subclasses must implement this method")

    def set_logger(self, logger):
        self._logger = logger


class NoNotificationAdapter(NotificationAdapter):
    def send_notification(self, message: str, log_level=logging.INFO):
        if self._logger:
            self._logger.log(log_level, message)


class SnsNotificationAdapter(NotificationAdapter):
    def __init__(self, topic_arn):
        self._sns_client = boto3.client("sns")
        self._topic_arn = topic_arn
        super().__init__()

    def send_notification(self, message: str, log_level=logging.INFO):
        if self._logger:
            self._logger.log(log_level, message)

        self._sns_client.publish(
            TopicArn=self._topic_arn,
            Message=message,
        )
