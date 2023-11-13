import os
import datetime
import requests

def job_done():
    # Get token from environment
    token = os.environ['SLACK_BOT_TOKEN']

    channel = 'nodes'
    texts = []
    texts.append(f'Job Done: {datetime.datetime.now()}')
    texts.append(f'    \n    From: {os.uname()}')
    text = '\n'.join(texts)
    bot_name = 'monitor'

    payload = data = {'token': token, 'channel': channel, 'text': text, 'username': bot_name}
    r = requests.post('https://slack.com/api/chat.postMessage', payload)
    #print(r.text)
