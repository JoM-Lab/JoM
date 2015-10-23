#!/usr/bin/env python3
import os
import readline
from query import Query

HISTORY = 'repl.history'


def main():
    print('type exit to quit repl')
    if os.path.exists(HISTORY):
        readline.read_history_file(HISTORY)
    q = Query()
    try:
        while True:
            l = input('> ').strip()
            if not l:
                continue
            elif l == 'exit':
                break
            else:
                res, _, _ = q.query(l)
                if isinstance(res, str):
                    res = res.strip()
                print(res)
    finally:
        readline.write_history_file(HISTORY)

if __name__ == '__main__':
    main()
