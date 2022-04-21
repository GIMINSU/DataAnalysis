import requests

def _post_message(self, text):
    response = requests.post("https://slack.com/api/chat.postMessage",
        headers={"Authorization": "Bearer "+ self.myToken},
        data={"channel": self.channel_name,"text": text}
    )
    if response.ok:
        print("Success. Send message with slack.")
    else:
        print("Fail. Send message with slack.")
        pass