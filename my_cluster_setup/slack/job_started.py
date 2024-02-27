import os
import datetime
import requests

import click


def send_message():
    # Get token from environment
    token = os.environ['SLACK_BOT_TOKEN']

    channel = 'nodes'
    texts = []
    texts.append(f'Bash started: {datetime.datetime.now()}')
    texts.append(f'    \n    From: {os.uname()}')
    texts.append(f'    \n    From: {os.system("sstat --format=AveCPU,AvePages,AveRSS,AveVMSize,JobID -j $SLURM_JOB_ID --allsteps")}')
    text = '\n'.join(texts)
    bot_name = 'monitor'

    payload = data = {'token': token, 'channel': channel, 'text': text, 'username': bot_name}
    r = requests.post('https://slack.com/api/chat.postMessage', payload)
    #print(r.text)
