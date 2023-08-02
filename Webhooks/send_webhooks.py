import requests
import json

webhook_url = 'http://127.0.0.1:8000/webhook'

data = {
    'name': 'Jane',
    'Channel_URL': 'https://www.youtube.com/channel/UC4Snw5yrSDMXys31I18U3gg'
}

r = requests.post(webhook_url, data=json.dumps(data), headers={'Content-Type': 'application/json'})

# Check the response status code to see if the request was successful
if r.status_code == 200:
    print("Webhook sent successfully.")
else:
    print(f"Failed to send webhook. Status code: {r.status_code}")

# Put this code into your terminal and run: python send_webhooks.py


