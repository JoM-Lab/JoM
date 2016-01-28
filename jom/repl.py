#!/usr/bin/env python3
import os
import sys
import readline
import traceback
from .query import Query

HISTORY = 'repl.history'


def main(debug):
    print('type exit to quit repl')
    if os.path.exists(HISTORY):
        readline.read_history_file(HISTORY)
    q = Query(debug=debug)
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
                    print(str(e))
                    traceback.print_tb(e.__traceback__)
                    continue
                if res.message:
                    print(res.message.strip())
                elif res.fileobj:
                    print(res.fileobj)
    except EOFError:
        return
    finally:
        readline.write_history_file(HISTORY)

if __name__ == '__main__':
    debug = len(sys.argv) > 1 and sys.argv[1] == 'debug'
    main(debug)
