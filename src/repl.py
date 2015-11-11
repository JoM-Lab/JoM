#!/usr/bin/env python3
import os
import sys
import readline
import traceback
from query import Query

HISTORY = 'repl.history'


def main():
    print('type exit to quit repl')
    if os.path.exists(HISTORY):
        readline.read_history_file(HISTORY)
    q = Query(debug=True)
    try:
        while True:
            l = input('> ').lstrip('/')
            if not l:
                continue
            elif l == 'exit':
                break
            else:
                try:
                    res = q.query('repl', l)
                except Exception as e:
                    traceback.print_tb(e.__traceback__)
                    continue
                if res.message:
                    print(res.message.strip())
                elif res.fileobj:
                    print(res.fileobj)
    finally:
        readline.write_history_file(HISTORY)

if __name__ == '__main__':
    debug = len(sys.argv) > 1 and sys.argv[1] == 'debug'
    main()
