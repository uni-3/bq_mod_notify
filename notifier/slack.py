from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from notifier.base import Notifier

class SlackNotifier(Notifier):

    def __init__(self, token):
        self.client = WebClient(token=token)

    def send_notification(self, channel, message):
        try:
            response = self.client.chat_postMessage(
                channel=channel,
                text=message
            )
        except SlackApiError as e:
            print(f"Error sending message: {e}")
