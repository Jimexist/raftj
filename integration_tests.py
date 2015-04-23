#!/usr/bin/env python
import os
import subprocess
import time

JAR = 'target/raftj-1.0-SNAPSHOT-jar-with-dependencies.jar'

def main():
    try:
        p = subprocess.Popen([
            'java',
            '-jar', JAR,
            'localhost:17001',
            'log.txt'
        ])
        p.wait()
    finally:
        p.terminate()

if __name__ == "__main__":
    main()