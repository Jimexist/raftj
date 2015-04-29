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
        print 'starting', i
        servers["localhost:{}".format(i)] = start_one_server(i)
    return servers

def main():

    servers = {}
    
    try:
        servers = start_all_servers()
        
        print "starting all servers"
        
        app = Flask(__name__, static_url_path='')

        @app.route("/")
        def index():
            return app.send_static_file('index.html')
            
        @app.route("/api/servers")
        def server_list():
            return json.dumps(servers.keys())
            
        @app.route("/api/servers/<address>/status")
        def get_status(address):
            resp, time, msg = send_command(address, '', follow=False)
            return json.dumps({
                'status': resp.success if resp else 'error',
                'leaderID': resp.leaderID if resp else '',
                'message': msg, 
                'time': time
            })   
            
        @app.route("/api/servers/<address>/command", methods=['POST'])
        def command(address):
            return 'not implemented'
    
        @app.route("/api/servers/<address>/start", methods=['POST'])
        def start_server(address):
            if address in servers:
                print "starting {}".format(address)
                server = servers[address]
                if server is None:
                    try:
                        servers[address] = start_one_server(address.split(":")[1])
                        return json.dumps({"message": "server '{}' is successfully started".format(address)})
                    except Exception:
                        print >> sys.stderr, 'error starting server', server
                else:
                    return json.dumps({"message": "server '{}' is currently running".format(address)})
            else:
                return json.dumps({"message": "server '{}' is not valid".format(address)})
                
        @app.route("/api/servers/<address>/restart", methods=['POST'])
        def restart_server(address):
            if address in servers:
                print "restarting {}".format(address)
                server = servers[address]
                if server:
                    try:
                        server.terminate()
                        server.wait()
                        servers[address] = start_one_server(address.split(":")[1])
                        return json.dumps({"message": "server '{}' is successfully restarted".format(address)})
                    except Exception:
                        print >> sys.stderr, 'error stoping server', server
                else:
                    return json.dumps({"message": "server '{}' is not currently running".format(address)})
            else:
                return json.dumps({"message": "server '{}' is not valid".format(address)})
    
        @app.route("/api/servers/<address>/stop", methods=['POST'])
        def stop_server(address):
            if address in servers:
                print "stopping {}".format(address)
                server = servers[address]
                if server:
                    try:
                        server.terminate()
                        server.wait()
                        servers[address] = None
                        return json.dumps({"message": "server '{}' is successfully stopped".format(address)})
                    except Exception:
                        print >> sys.stderr, 'error stoping server', server
                else:
                    return json.dumps({"message": "server '{}' is not currently running".format(address)})
            else:
                return json.dumps({"message": "server '{}' is not valid".format(address)})

        app.run(debug=False)
        
    finally:
        for k, p in servers.iteritems():
            try:                
                p.terminate()
                p.wait()
            except Exception:
                print >> sys.stderr, 'error stoping server', k

if __name__ == "__main__":
    main()
