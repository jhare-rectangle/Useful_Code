from slack_sdk.webhook import WebhookClient


class SlackClient:
    def __init__(self, hook_url):
        self._hook_url = hook_url

    def send_message(self, fallback_message, status_message, details, icon_name=None):
        """
        Send a message to the Slack channel associated with the configured webhook URL.  `status_message` and
         `details` maybe be Markdown formatted.
        :param fallback_message: Simple message for fallback
        :param status_message: Overall status message
        :param details: Additional details for report.
        :param icon_name: Optional emoji shortname to punctuate message.  If it's invalid, that's on you
        :return: True for success, False otherwise
        """
        webhook = WebhookClient(self._hook_url)
        if isinstance(icon_name, str) and len(icon_name) > 2:
            if icon_name[0] != ':' and icon_name[-1] != ":":
                icon_name = f":{icon_name}: "
            else:
                icon_name = ""
        else:
            icon_name = ""
        response = webhook.send(
            text=fallback_message,
            blocks=[
                {"type": "section",
                 "text": {"type": "mrkdwn",
                          "text": f"{icon_name}{status_message}"}
                 },
                {"type": "section",
                 "text": {"type": "mrkdwn",
                          "text": details}
                 }
            ]
        )
        return response.status_code == 200 and response.body == "ok"
