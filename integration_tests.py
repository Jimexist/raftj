#!/usr/bin/env python
import os
import subprocess
import time

JAR = 'target/raftj-1.0-SNAPSHOT-jar-with-dependencies.jar'

def start_all_servers():
    servers = []
    for i in ['17001','17002','17003','17004','17005']:
        p = subprocess.Popen([
            'java',
            '-jar', JAR,
            'localhost:{}'.format(i),
            '{}.log'.format(i),
        ])
        servers.append(p)
    return servers

def main():
    try:
        servers = start_all_servers()
        time.sleep(100)
    finally:
        for p in servers:
            p.terminate()
            p.wait()

if __name__ == "__main__":
    main()