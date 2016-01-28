#!/usr/bin/env python3
import sys
import json
import requests


class Sender:
    '''
    Send message to `Telegram`.
    '''

    def __init__(self, config_file='config.json'):
        '''
        :param str config_file: path to `config.json`
        '''
        self.config = json.load(open(config_file))
        self.prefix = 'https://api.telegram.org/bot' + self.config['tg_key']
        self.hide_keyboard = {"hide_keyboard": True, "selective": True}

    @staticmethod
    def debug(tp, msg):
        '''
        :param tp: type of debug info
        :param msg: message
        '''
        sys.stdout.write("{}: {}\n".format(tp, msg))
        sys.stdout.flush()

    def send_resp(self, cid, resp, mid=None):
        '''
        :param int cid: who to receive this message
        :param resp: object of `Resp`
        :type resp: Resp
        :param mid: reply to which message
        :type mid: int | None
        :rtype: bool
        '''

        # must be string
        keyboard = json.dumps(resp.keyboard or self.hide_keyboard)
        if resp.inline is not None:
            return self.request('answerInlineQuery', resp.inline)

        elif resp.message is not None:
            data = dict(chat_id=cid, text=resp.message, reply_to_message_id=mid,
                        disable_web_page_preview=not resp.preview,
                        reply_markup=keyboard)
            if resp.markdown:
                data['parse_mode'] = "Markdown"
            return self.request('sendMessage', data)

        elif resp.fileobj is not None:
            if resp.fileobj.name.endswith('.png'):
                data = dict(chat_id=cid, reply_markup=keyboard)
                files = dict(photo=resp.fileobj)
                return self.request('sendPhoto', data, files)
            else:
                self.debug("unknown file type:", resp.fileobj.name)
                return False

        else:
            self.debug("unknown send_resp:", (resp.message, resp.fileobj))
            return False

    def request(self, method, dt, fl=None):
        '''
        :param str method: `sendMessage` or `sendPhoto`
        :param dt: a dict of data
        :type dt: Dict[str, str]
        :param fl: file objects to send
        :type fl: Dict[str, File]
        :rtype: bool
        '''
        url = self.prefix + '/' + method
        self.debug(method, dt)
        try:
            result = requests.post(url, data=dt, files=fl)
            self.debug('result of ' + method, result.text)
            result = result.json()
            if not result['ok']:
                err_msg = 'Error {}: {}'.format(result['error_code'], result['description'])
                if 'chat_id' in dt:
                    requests.post(self.prefix + '/sendMessage',
                                  data=dict(chat_id=dt['chat_id'], text=err_msg))
            return result['ok']
        except Exception as e:
            self.debug('error in request', str(e))
            return False
