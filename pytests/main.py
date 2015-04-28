#!/usr/bin/env python
import os
import copy
import subprocess
import socket
import sys

def load_classes():
    OUTPUT_FILE = 'messages_pb2.py'
    SOURCE_DIR = '../src/main/protobuf'
    SOURCE_PROTO = os.path.join(SOURCE_DIR, 'messages.proto')
    if not os.path.exists(OUTPUT_FILE):
        subprocess.check_call([
            'protoc',
            '-I={}'.format(SOURCE_DIR),
            '--python_out={}'.format('.'),
            SOURCE_PROTO
        ])

def send_command(hostname, port, command):
    with sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM):
        server_address = (hostname, port)
        sock.connect(server_address)
        try:
            req = messages_pb2.Request()
            req.command = command
            
            data = req.SerializeToString()
            sock.write(struct.pack("H", len(data)))
            sock.write(data)
            
            dataToRead = struct.unpack("H", socket.read(2))[0]
            dataRead = socket.read(dataToRead)
            resp = messages_pb2.ClientMessageResponse()
            resp.ParseFromString(dataRead)
            return a.success, a.leaderID

load_classed()