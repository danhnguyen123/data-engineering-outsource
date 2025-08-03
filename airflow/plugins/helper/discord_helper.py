import requests


def send_discord_message(webhook_url, content):
    data = {
        "content": content  # Message content
    }
    response = requests.post(webhook_url, json=data)