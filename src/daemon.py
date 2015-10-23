#!/usr/bin/env python3
import os
import sys
import time
import shlex
import bottle
import subprocess

UPDATE_CMD = 'python src/update.py'
POLLING_CMD = 'python src/polling.py'
BRANCH_CMD = 'git branch | grep "*" | cut -d " " -f 2'
HEAD_CMD = 'git log -n 1 --oneline'
update_proc = None
update_err_msg = ''
polling_proc = None
polling_err_msg = ''


def restart_procs():
    global update_proc, update_err_msg, polling_proc, polling_err_msg

    if update_proc and update_proc.poll() is None:
        sys.stderr.write('kill update proc {}\n'.format(update_proc.pid))
        update_proc.terminate()
    update_proc = subprocess.Popen(shlex.split(UPDATE_CMD),
                                   stdout=open('update.log', 'a', 1),
                                   stderr=subprocess.PIPE)
    sys.stderr.write('start update proc {}\n'.format(update_proc.pid))
    update_err_msg = ''  # reset

    if polling_proc and polling_proc.poll() is None:
        sys.stderr.write('kill polling proc {}\n'.format(polling_proc.pid))
        polling_proc.terminate()
    polling_proc = subprocess.Popen(shlex.split(POLLING_CMD),
                                    stdout=open('polling.log', 'a', 1),
                                    stderr=subprocess.PIPE)
    sys.stderr.write('start polling proc {}\n'.format(polling_proc.pid))
    polling_err_msg = ''  # reset

    time.sleep(1)
    check_status()


def check_status():
    global update_err_msg, polling_err_msg
    if not update_proc:
        update_status = 'not started'
    elif update_proc.poll() is None:
        update_status = 'running({})'.format(update_proc.pid)
    else:  # dead
        update_status = 'exited({})'.format(update_proc.returncode)
        if update_err_msg == '':
            update_err_msg = update_proc.stderr.read().decode('utf-8')

    if not polling_proc:
        polling_status = 'not started'
    elif polling_proc.poll() is None:
        polling_status = 'running({})'.format(polling_proc.pid)
    else:  # dead
        polling_status = 'exited({})'.format(polling_proc.returncode)
        if polling_err_msg == '':
            polling_err_msg = polling_proc.stderr.read().decode('utf-8')

    return update_status, polling_status


@bottle.get('/hook')
@bottle.post('/hook')
def hook():
    # data = bottle.request.json
    os.system('git pull')
    restart_procs()


@bottle.route('/')
def main():
    (st, branch) = subprocess.getstatusoutput(BRANCH_CMD)
    (st, head) = subprocess.getstatusoutput(HEAD_CMD)
    update_status, polling_status = check_status()
    return '''\
<html><body>
current branch: {branch}<br/>
current HEAD: {head}<br/><br/>
update proc: {update_status}<br/>
<pre>{update_err_msg}</pre><br/>
pollinging proc: {polling_status}<br/>
<pre>{polling_err_msg}</pre><br/>
</body></html>
'''.format(branch=branch,
           head=head,
           update_status=update_status,
           update_err_msg=update_err_msg,
           polling_status=polling_status,
           polling_err_msg=polling_err_msg)


if __name__ == '__main__':
    restart_procs()
    bottle.run(host='', port=8888, debug=True)
