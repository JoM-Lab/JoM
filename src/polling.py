#!/usr/bin/env python3
import os
import sys
import json
import opencc
import asyncio
import requests
from query import Query
from sender import Sender
from resp import Resp


class Telegram:
    '''
    Fetch query messages from `telegram` with long polling.
    '''

    def __init__(self, config_file='config.json'):
        '''
        :param str config_file: path to `config.json`
        '''
        self.config = json.load(open(config_file))
        self.prefix = 'https://api.telegram.org/bot' + self.config['tg_key']
        self.sender = Sender(config_file)
        self.timeout = 60
        self.allowed = self.config['allowed']
        self.q = Query(self.config)

    @staticmethod
    def debug(tp, msg):
        '''
        :param tp: type of debug info
        :param msg: message
        '''
        sys.stdout.write("{}: {}\n".format(tp, msg))
        sys.stdout.flush()

    def notifier(self):
        '''
        :return: coroutine to deal with notification
        :rtype: asyncio.coroutine
        '''
        @asyncio.coroutine
        def notify(reader, _writer):
            message = yield from reader.read(1024)
            if message:
                message = message.decode('utf8')
                self.debug('sock', 'recieved ' + message)
                try:
                    resp = Resp(**json.loads(message))
                except Exception as e:
                    self.debug('error in sock', str(e))
                    resp = Resp(message='error in notification: ' + str(e))
                self.sender.send_resp(self.config['default_notify_id'], resp)
                self.debug('sock', 'notified')
            _writer.close()
        return notify

    @asyncio.coroutine
    def polling(self, loop):
        '''
        :param loop: the default event loop
        :type loop: asyncio.BaseEventLoop
        '''
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
                mid = m['message_id']
                cid = m['chat']['id']
                if cid in self.allowed and 'text' in m and m['text'][0] == '/':
                    m['text'] = opencc.convert(m['text'])
                    resp = self.q.query(cid, m['text'][1:])
                    self.sender.send_resp(cid, resp, mid)
                else:
                    self.sender.send_resp(cid, Resp(message='mew?'))
                offset = r['update_id'] + 1

    def run(self):
        '''
        Listen to events from polling and notifying.
        '''
        try:
            os.remove("/tmp/jom")
        except OSError:
            pass
        loop = asyncio.get_event_loop()
        server = loop.run_until_complete(
            asyncio.start_unix_server(self.notifier(), path="/tmp/jom", loop=loop))
        self.debug('Serving', server.sockets[0].getsockname())
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
