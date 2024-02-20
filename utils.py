import logging
from .json_to_snowflake import new_file_name
from slack import WebClient
path = "callcap/log"


#Configuring Logging
logging.basicConfig(
        level = logging.INFO,
        format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt= "%y-%m-%d %H:%M:%S",
        filename=f"{path}/{new_file_name}.log"
    )


def send_slack_notification(token, channel, message):
    """
    Sends a Slack notification to a specified channel using the provided Slack API token.

    Args:
        token (str): The Slack API token required for authentication.
        channel (str): The ID or name of the channel to send the notification to.
        message (str): The message to be sent in the Slack notification.

    Returns:
        None

    """

    client = WebClient(token=token)

    response = client.chat_postMessage(
        channel=channel,
        text=message
    )
    success = response.data['ok']
        
    if success:
        logging.info("Slack notification sent successfully!")
    else:
        logging.info("Failed to send Slack notification.")

    
