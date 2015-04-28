#!/usr/bin/env python
import os
import sys
import subprocess
import time
import json
import random
from raftjclient import send_command
from flask import Flask, send_from_directory, request

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
        
        app = Flask(__name__, static_url_path='')

        @app.route("/")
        def index():
            return app.send_static_file('index.html')
            
        @app.route("/api/servers")
        def server_list():
            return json.dumps(servers.keys())
    
        @app.route("/api/stop/<address>", methods=['POST'])
        def stop():
            if address in servers:
                server = servers[address]
                if server:
                    try:
                        server.terminate()
                        server.wait()
                        servers[address] = None
                        return json.dumps({"message": "server {} is successfully stopped".format(server)})
                    except Exception:
                        print >> sys.stderr, 'error stoping server', server
                
            return json.dumps({"message": "server {} is not running".format(server)})

        app.run(debug=True)
        
    finally:
        for k, p in servers.iteritems():
            try:                
                p.terminate()
                p.wait()
            except Exception:
                print >> sys.stderr, 'error stoping server', k

if __name__ == "__main__":
    main()
