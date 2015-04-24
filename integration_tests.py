#!/usr/bin/env python
import os
import subprocess
import time
import random
from threading import Thread

JAR = 'target/raftj-1.0-SNAPSHOT-jar-with-dependencies.jar'
CLIENT_CLASS = 'edu.cmu.raftj.client.Client'


def send_commands(client):
    client.communicate("\n".join(map(str, xrange(1000))))

def start_client(port):
    return subprocess.Popen([
        'java',
        '-cp', JAR,
        CLIENT_CLASS,
        'localhost:{}'.format(port)
    ], stdin=subprocess.PIPE)

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
    client = None
    try:
        servers = start_all_servers()
        time.sleep(1)
        
        client = start_client(17003)
        thread = Thread(target=send_commands, args=(client,))
        thread.start()
        
        for i in xrange(2):
            to_stop = random.sample(servers.keys(), 2)
            for k in to_stop:
                servers[k].terminate()
                servers[k].wait()
            time.sleep(1)
            for k in to_stop:
                servers[k] = start_one_server(k)
            time.sleep(5)
                
        thread.join()
    finally:
        for k, p in servers.iteritems():
            try:                
                p.terminate()
                p.wait()
            except Exception:
                pass
        try:
            client.terminate()
            client.wait()
        except Exception:
            pass

if __name__ == "__main__":
    main()