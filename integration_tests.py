#!/usr/bin/env python
import os
import subprocess
import time
import random

JAR = 'target/raftj-1.0-SNAPSHOT-jar-with-dependencies.jar'

def start_one_server(port):
    return subprocess.Popen([
            'java',
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
    servers = start_all_servers()
    try:
        for i in xrange(5):
            time.sleep(3)
            to_stop = random.sample(servers.keys(), 2)
            for k in to_stop:
                servers[k].terminate()
                servers[k].wait()
            time.sleep(3)
            for k in to_stop:
                servers[k] = start_one_server(k)
            
    finally:
        for k, p in servers.iteritems():
            p.terminate()
            p.wait()

if __name__ == "__main__":
    main()