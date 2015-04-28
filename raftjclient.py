#!/usr/bin/env python
import os
import copy
import subprocess
import socket
import sys
import struct
import time
from google.protobuf.internal import encoder, decoder

def load_classes():
    OUTPUT_FILE = './messages_pb2.py'
    SOURCE_DIR = '../src/main/protobuf'
    SOURCE_PROTO = os.path.join(SOURCE_DIR, 'messages.proto')
    if not os.path.exists(OUTPUT_FILE):
        subprocess.check_call([
            'protoc',
            '-I={}'.format(SOURCE_DIR),
            '--python_out={}'.format('.'),
            SOURCE_PROTO
        ])

load_classes()
import messages_pb2

def socket_read_n(sock, n):
    """ Read exactly n bytes from the socket.
        Raise RuntimeError if the connection closed before
        n bytes were read.
    """
    buf = ''
    while n > 0:
        data = sock.recv(n)
        if data == '':
            raise RuntimeError('unexpected connection close')
        buf += data
        n -= len(data)
    return buf

def send_command(hostport, command):
    sent = False
    while not sent:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            host, port = hostport.split(':')
            sock.connect((host, int(port)))
        
            req = messages_pb2.Request()
            req.command = command
            data = req.SerializeToString()
            packed_len = encoder._VarintBytes(len(data))
            
            start = time.time()
            sock.sendall(packed_len + data)
        
            len_buf = socket_read_n(sock, 1)
            size, position = decoder._DecodeVarint(len_buf, 0)
            msg_buf = socket_read_n(sock, size)
            
            end = time.time()
            
            resp = messages_pb2.ClientMessageResponse()
            resp.ParseFromString(msg_buf)
            sent, hostport = resp.success, resp.leaderID
        except Exception as e:
            pass
            # print >> sys.stderr, 'exception', e
        finally:
            sock.close()
    return end - start
