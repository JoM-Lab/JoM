#!/usr/bin/env python3
import sys
import json
import requests


class Sender:
    def __init__(self, config_file='config.json'):
        self.config = json.load(open(config_file))
        self.prefix = 'https://api.telegram.org/bot' + self.config['tg_key']
        self.selective = False
        self.hide_keyboard = {"hide_keyboard": True,
                              "selective": self.selective}

    @staticmethod
    def debug(tp, msg):
        sys.stdout.write("{}: {}\n".format(tp, msg))
        sys.stdout.flush()

    def send_resp(self, cid, reply, keyboard, no_preview):
        keyboard = json.dumps(keyboard)
        if isinstance(reply, str):
            data = dict(chat_id=cid, text=reply, disable_web_page_preview=no_preview,
                        reply_markup=keyboard)
            self.request('sendMessage', data)

        elif hasattr(reply, 'fileno'):
            if reply.name.endswith('.png'):
                data = dict(chat_id=cid, reply_markup=keyboard)
                files = dict(photo=reply)
                self.request('sendPhoto', data, files)
            elif reply.name.endswith('.mp3'):
                data = dict(chat_id=cid, reply_markup=keyboard)
                files = dict(audio=reply)
                self.request('sendAudio', data, files)

        else:
            self.debug("unknown send_resp:", (reply, type(reply)))

    def request(self, method, dt, fl=None):
        url = self.prefix + '/' + method
        self.debug(method, dt)
        j = requests.post(url, data=dt, files=fl)
        self.debug('result of ' + method, j.text)
