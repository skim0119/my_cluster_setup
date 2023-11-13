import os
import requests

def job_done():
    # Get token from environment
    token = os.environ['SLACK_BOT_TOKEN']

    channel = 'nodes'
    text = f'Job Done.\n    From: {os.uname()}'
    bot_name = 'monitor'

    payload = data = {'token': token, 'channel': channel, 'text': text, 'username': bot_name}
    r = requests.post('https://slack.com/api/chat.postMessage', payload)
    #print(r.text)
