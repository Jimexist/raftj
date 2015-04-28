#!/usr/bin/env python
import os
import sys
import subprocess
import time
import random
from raftjclient import send_command

JAR = 'target/raftj-1.0-SNAPSHOT-jar-with-dependencies.jar'

def start_one_server(port):
    return subprocess.Popen([
        'java',
        '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn',
        '-ea',
        '-jar', JAR,
        'localhost:{}'.format(port),
        '{}.log'.format(port),
    ])

def start_all_servers():
    servers = {}
    for i in ['17001','17002','17003','17004','17005']:
        servers[i] = start_one_server(i)
    return servers

def main():
    servers = {}
    try:
        servers = start_all_servers()
        time.sleep(5)
        
        print send_command('127.0.0.1:17003', 'hello')
        time.sleep(1)
        print send_command('127.0.0.1:17001', 'world')
        time.sleep(1)
        print send_command('127.0.0.1:17005', 'lol')
        time.sleep(1)
        
        # for i in xrange(2):
        #     to_stop = random.sample(servers.keys(), 2)
        #     for k in to_stop:
        #         servers[k].kill()
        #         servers[k].wait()
        #     time.sleep(1)
        #
        #     send_command('127.0.0.1:17002', 'tuple')
        #     time.sleep(1)
        #
        #     for k in to_stop:
        #         servers[k] = start_one_server(k)
        #     time.sleep(5)
        #
        #     send_command('127.0.0.1:17002', 'again')
        #     time.sleep(1)

    finally:
        for k, p in servers.iteritems():
            try:                
                p.terminate()
                p.wait()
            except Exception:
                pass

if __name__ == "__main__":
    main()