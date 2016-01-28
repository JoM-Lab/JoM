#!/usr/bin/env python3
from gevent import monkey
monkey.patch_all()
import sys
import time
import shlex
import bottle
import subprocess

UPDATE_CMD = 'python -m jom.update'
POLLING_CMD = 'python -m jom.polling'
DOCS_CMD = 'cd docs; make html'
# cmd to get the current branch
BRANCH_CMD = 'git branch | grep "*" | cut -d " " -f 2'
# cmd to get the newest commit log
HEAD_CMD = 'git log -n 1 --oneline'
update_proc = None
update_err_msg = ''
polling_proc = None
polling_err_msg = ''


def restart_procs():
    '''
    restart update and polloing processes.
    '''
    global update_proc, update_err_msg, polling_proc, polling_err_msg

    if update_proc and update_proc.poll() is None:
        # if running, kill it first
        sys.stderr.write('kill update proc {}\n'.format(update_proc.pid))
        update_proc.terminate()
    update_proc = subprocess.Popen(shlex.split(UPDATE_CMD),
                                   stdout=open('update.log', 'a', 1),
                                   stderr=subprocess.PIPE)
    sys.stderr.write('start update proc {}\n'.format(update_proc.pid))
    # reset the error messages
    update_err_msg = ''

    if polling_proc and polling_proc.poll() is None:
        # if running, kill it first
        sys.stderr.write('kill polling proc {}\n'.format(polling_proc.pid))
        polling_proc.terminate()
    polling_proc = subprocess.Popen(shlex.split(POLLING_CMD),
                                    stdout=open('polling.log', 'a', 1),
                                    stderr=subprocess.PIPE)
    sys.stderr.write('start polling proc {}\n'.format(polling_proc.pid))
    # reset the error messages
    polling_err_msg = ''

    time.sleep(1)
    # check if dead immediately and get error messages
    check_status()


def check_status():
    '''
    Check the status of process and pull out error messages.
    '''
    global update_err_msg, polling_err_msg
    if not update_proc:
        update_status = 'not started'
    elif update_proc.poll() is None:
        update_status = 'running({})'.format(update_proc.pid)
    else:  # dead
        update_status = 'exited({})'.format(update_proc.returncode)
        if update_err_msg == '':  # haven't pull out
            update_err_msg = update_proc.stderr.read().decode('utf-8')

    if not polling_proc:
        polling_status = 'not started'
    elif polling_proc.poll() is None:
        polling_status = 'running({})'.format(polling_proc.pid)
    else:  # dead
        polling_status = 'exited({})'.format(polling_proc.returncode)
        if polling_err_msg == '':  # haven't pull out
            polling_err_msg = polling_proc.stderr.read().decode('utf-8')

    # regenerate docs
    subprocess.getstatusoutput(DOCS_CMD)

    return update_status, polling_status


@bottle.post('/checkout')
def checkout():
    '''
    Change current branch.
    '''
    branch = bottle.request.forms.get('branch')
    (st, output) = subprocess.getstatusoutput('git checkout ' + branch)
    if st != 0:
        return 'error {} in pull: {}'.format(st, output)
    restart_procs()
    return '{} {}'.format(*check_status())


@bottle.get('/hook')
@bottle.post('/hook')
def hook():
    '''
    Pull commits from remote server and restart processes.
    '''
    # data = bottle.request.json
    (st, output) = subprocess.getstatusoutput('git pull')
    if st != 0:
        return 'error {} in pull: {}'.format(st, output)
    restart_procs()
    return '{} {}'.format(*check_status())


@bottle.route('/docs/<p:re:.*>')
@bottle.auth_basic(lambda username, password:
        username == 'jom' and password == 'moj')
def serve_docs(p=''):
    if not p:
        p = 'index.html'
    return bottle.static_file(p, root='docs/_build/html')


@bottle.route('/')
@bottle.auth_basic(lambda username, password:
        username == 'jom' and password == 'moj')
def main():
    '''
    Display HTML.
    '''
    (st, branch) = subprocess.getstatusoutput(BRANCH_CMD)
    (st, head) = subprocess.getstatusoutput(HEAD_CMD)
    update_status, polling_status = check_status()
    return '''\
<html><body>
<form action="/checkout" method="post">
current branch: {branch}
<input name="branch" type="text"/>
<input value="checkout" type="submit"/></form><br/>
current HEAD: {head}
<a href="/hook"><button type="button">pull and restart</button></a><br/><br/>
update proc: {update_status}<br/>
<pre>{update_err_msg}</pre><br/>
pollinging proc: {polling_status}<br/>
<pre>{polling_err_msg}</pre><br/>
<a href="/docs/">docs</a>
</body></html>
'''.format(branch=branch,
           head=head,
           update_status=update_status,
           update_err_msg=update_err_msg,
           polling_status=polling_status,
           polling_err_msg=polling_err_msg)


if __name__ == '__main__':
    restart_procs()
    bottle.run(host='', port=8888, debug=True, server='gevent')
