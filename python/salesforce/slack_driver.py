import os
from dotenv import load_dotenv
from slack_api.slack_client import SlackClient


if __name__ == "__main__":
    load_dotenv()
    slack_hook_url = os.getenv("slack_hook_url")
    slack = SlackClient(slack_hook_url)
    response = slack.send_message("Fake test message", "Status ```IGNORE```",
                                  "This was just a test", icon_name=":snowflake:")
