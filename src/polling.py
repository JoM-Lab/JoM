#!/usr/bin/env python3
import os
import sys
import json
import opencc
import asyncio
import requests
from query import Query
from sender import Sender


class Telegram:

    def __init__(self, config_file='config.json'):
        self.config = json.load(open(config_file))
        self.prefix = 'https://api.telegram.org/bot' + self.config['tg_key']
        self.sender = Sender(config_file)
        self.timeout = 60
        self.allowed = self.config['allowed']
        self.q = Query(self.config)

    @staticmethod
    def debug(tp, msg):
        sys.stdout.write("{}: {}\n".format(tp, msg))
        sys.stdout.flush()

    def notifier(self):
        @asyncio.coroutine
        def notify(reader, _writer):
            while 1:
                message = yield from reader.read(8192)
                if not message:
                    break
                self.sender.send_resp(
                    self.config['default_notify_id'], message.decode("utf8"),
                    self.sender.hide_keyboard, True)
                print("notified:", message)
        return notify

    @asyncio.coroutine
    def polling(self, loop):
        offset = 0
        while True:
            try:
                req = yield from loop.run_in_executor(
                    None, lambda: requests.post(self.prefix + '/getUpdates', timeout=None,
                                                data=dict(offset=offset, timeout=self.timeout)))
                j = req.json()
            except ValueError:
                self.debug("ERROR", req.text)
                continue
            if not j['ok'] or not j['result']:
                continue
            self.debug("receive", json.dumps(j))
            for r in j['result']:
                m = r['message']
                self.debug("message", json.dumps(m))
                cid = m['chat']['id']
                if cid in self.allowed and 'text' in m and m['text'][0] == '/':
                    m['text'] = opencc.convert(m['text'])
                    reply, keyboard, no_preview = self.q.query(m['text'][1:])
                    self.sender.send_resp(cid, reply, keyboard, no_preview)
                else:
                    self.sender.send_resp(cid, 'mew?', self.q.hide_keyboard, True)
                offset = r['update_id'] + 1

    def run(self):
        try:
            os.remove("/tmp/jom")
        except OSError:
            pass
        loop = asyncio.get_event_loop()
        server = loop.run_until_complete(
            asyncio.start_unix_server(self.notifier(), path="/tmp/jom", loop=loop))
        loop.run_until_complete(self.polling(loop))
        try:
            loop.run_forever()
        finally:
            server.close()
            loop.run_until_complete(server.wait_closed())
            loop.close()

if __name__ == '__main__':
    tg = Telegram()
    try:
        tg.run()
    except KeyboardInterrupt:
        exit()
