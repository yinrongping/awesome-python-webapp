#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 用于监听代码修改，自动重启应用
# easy_install watchdog
__author__ = 'Michael Liao'

import os
import sys
import time
import subprocess

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


def log(s):
    print '[Monitor] %s' % s


class MyFileSystemEventHander(FileSystemEventHandler):

    def __init__(self, fn):
        super(MyFileSystemEventHander, self).__init__()
        self.restart = fn

    # 设置监听
    def on_any_event(self, event):
        # 监听任何的修改,如果是py文件的话，就重启应用
        if event.src_path.endswith('.py'):
            log('Python source file changed: %s' % event.src_path)
            self.restart()

command = ['echo', 'ok']
process = None


# 杀掉进程
def kill_process():
    global process
    if process:
        log('Kill process [%s]...' % process.pid)
        process.kill()
        process.wait()
        log('Process ended with code %s.' % process.returncode)
        process = None


# 启动项目
def start_process():
    global process, command
    log('Start process %s...' % ' '.join(command))
    process = subprocess.Popen(
        command, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr)


# 重启项目
def restart_process():
    kill_process()
    start_process()


# 开启监听模式
def start_watch(path, callback):
    observer = Observer()
    observer.schedule(
        MyFileSystemEventHander(restart_process), path, recursive=True)
    observer.start()
    log('Watching directory %s...' % path)
    start_process()
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == '__main__':
    # argv = sys.argv[1:]
    argv = ['wsgiapp.py']
    if not argv:
        print('Usage: ./pymonitor your-script.py')
        exit(0)
    if argv[0] != 'python':
        argv.insert(0, 'python')
    command = argv
    path = os.path.abspath('.')
    start_watch(path, None)
