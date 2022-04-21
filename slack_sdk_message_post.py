import logging
import os

# Import WebClient from Python SDK
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# WebClient instantiates a client that can call API methods
# When using Bolt, you can use either 'app.client' or the 'client' passed to listeners
def _post_message_with_slack_sdk(self, text=None, blocks=None):
    client = WebClient(token=self.myToken)
    logger = logging.getLogger(__name__)

    channel_id = self.channel_id

    try:
        # Call the conversations. list method using the WebClient
        if text is not None:
            result = client.chat_postMessage(
                channel=channel_id,
                text = text
            )
        elif blocks is not None:
            result = client.chat_postMessage(
                channel=channel_id,
                blocks = blocks
            )

    except SlackApiError as e:
        print(f"Error: {e}")


