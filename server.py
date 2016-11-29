#! /usr/bin/env python3

import os
import sys
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
import socket
import socketserver
import subprocess
import threading
import json

from google.protobuf.json_format import MessageToJson, Parse

from functools import partial

import mesos.v1.agent.agent_pb2 as pba
import mesos.v1.mesos_pb2 as pbm

from dcos import recordio

PORT = 8888


containers = {}
tasksToContainer = {"1":"a", "2":"b"}

def record_parse(message):
    msg = pba.Call()
    try:
        Parse(message, msg)
    except:
        msg = pba.Call.AttachContainerInput()
        Parse(message, msg)
    return msg

encoder = recordio.Encoder(lambda s: bytes(MessageToJson(s), "UTF-8"))
decoder = recordio.Decoder(record_parse)


ROUTES = [
    ('/mesos/master/api/v1')
]

class StreamingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)


class StreamingRequestHandler(BaseHTTPRequestHandler, object):
    def __init__(self, request, client_address, server):
        super(StreamingRequestHandler, self).__init__(
            request,
            client_address,
            server)

    def do_GET(self):
        if self.path == "/mesos/master/state.json":
            return self.get_state()

        self.send_response(400)

    def get_state(self):
        state = {
            'slaves': [{
                'hostname': 'localhost',
                'id': '1', }],
            'frameworks': [{
                'active':True, 
                'id': '1',
                'tasks': [],
                'completed_tasks': [],
                }]}

        for taskID, containerID in tasksToContainer.items():
            task = {
                'id': taskID,
                'slave_id': '1',
                'statuses': [{
                    'container_status': {
                        'container_id': {
                            'value': containerID }}}]}

            state['frameworks'][0]['tasks'].append(task)

        jsonState = json.dumps(state)

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Content-length', len(jsonState))
        self.end_headers()
        self.wfile.write(bytes(jsonState, 'utf-8'))

    def do_POST(self):
        if self.path == "/slave/1/api/v1":
            self.post_api_v1()
        else:
            self.send_response(400)            

    def post_api_v1(self):
        if 'content-length' in self.headers:
            length = int(self.headers['content-length'])
            content = self.rfile.read(length)

            records = decoder.decode(content)
            if records:
                for record in records:
                    if record.type == pba.Call.LAUNCH_NESTED_CONTAINER_SESSION:
                        return self.handle_launch_nested_container_session(record)
                    if record.type == pba.Call.ATTACH_CONTAINER_OUTPUT:
                        return self.handle_attach_container_output_stream(record)

        return self.handle_attach_container_input_stream()

    def handle_launch_nested_container_session(self, msg):
        container_id = msg.launch_nested_container_session.container_id.value

        if container_id in containers.keys():
            self.send_response(409)
            return

        stdin_pipe = os.pipe()
        stdout_pipe = os.pipe()
        stderr_pipe = os.pipe()

        process = subprocess.Popen(
            [msg.launch_nested_container_session.command.value] + list(msg.launch_nested_container_session.command.arguments),
            close_fds=True,
            env={},
            stdin=stdin_pipe[0],
            stdout=stdout_pipe[1],
            stderr=stderr_pipe[1])

        os.close(stdin_pipe[0])
        os.close(stdout_pipe[1])
        os.close(stderr_pipe[1])

        containers[container_id] = {
            "cmd" : msg.launch_nested_container_session.command,
            "args" : msg.launch_nested_container_session.command.arguments,
            "stdin_pipe" : stdin_pipe,
            "stdout_pipe" : stdout_pipe,
            "stderr_pipe" : stderr_pipe,
            "process" : process,
            "refcount" : 0,
            "lock" : threading.Lock(),
            "exit_event" : threading.Event()
        }

        msg = pba.Call()
        msg.type = pba.Call.ATTACH_CONTAINER_OUTPUT
        msg.attach_container_output.container_id.value = container_id

        self.incref(container_id)
        containers[container_id]["exit_event"].clear()

        self.handle_attach_container_output_stream(msg)
        process.wait()

    def handle_attach_container_output_stream(self, msg):
        self.send_response(200)
        self.send_header('Content-type', 'application/x-protobuf')
        self.send_header('transfer-encoding', 'chunked')
        self.end_headers()

        container_id = msg.attach_container_output.container_id.value

        stdin_write = containers[container_id]["stdin_pipe"][1]
        stdout_read = containers[container_id]["stdout_pipe"][0]
        stderr_read = containers[container_id]["stderr_pipe"][0]

        for chunk in iter(partial(os.read, stdout_read, 1024), b''):
            io_msg = pba.ProcessIO()
            io_msg.type = pba.ProcessIO.Type.Value('DATA')
            io_msg.data.type = pba.ProcessIO.Data.Type.Value('STDOUT')
            io_msg.data.data = chunk
            io_msg = encoder.encode(io_msg)
            self.send_chunked_msg(io_msg)

        for chunk in iter(partial(os.read, stderr_read, 1024), b''):
            io_msg = pba.ProcessIO()
            io_msg.type = pba.ProcessIO.Type.Value('DATA')
            io_msg.data.type = pba.ProcessIO.Data.Type.Value('STDERR')
            io_msg.data.data = chunk
            io_msg = encoder.encode(io_msg)
            self.send_chunked_msg(io_msg)

        os.close(stdout_read)
        os.close(stderr_read)

        self.send_chunked_terminator()

        containers[container_id]["exit_event"].set()
        self.decref(container_id)

    def handle_attach_container_input_stream(self):
        try:
            chunk = self.get_chunked_msg()
        except Exception as exception:
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            return

        container_id = chunk.attach_container_input.container_id.value
        stdin_write = containers[container_id]["stdin_pipe"][1]
        error_code = 200

        try:
            #TODO: Fix timeout
            # self.socket.settimeout(1)
            while True:
                try:
                    msg = self.get_chunked_msg()
                except Exception as exception:
                    continue

                data = msg.process_io.data
                if len(data.data) == 0:
                    break

                os.write(stdin_write, data.data)
        except Exception as exception:
            error_code = 400

        os.close(stdin_write)

        self.incref(container_id)
        containers[container_id]["exit_event"].wait()
        self.decref(container_id)

        self.send_response(error_code)
        self.send_header('Content-type', 'application/x-protobuf')
        self.end_headers()

    def send_chunked_msg(self, msg):
        chunk = '%X\r\n%s\r\n' % (len(msg), msg.decode('utf-8'))
        self.wfile.write(bytes(chunk, 'utf-8'))

    def send_chunked_terminator(self):
        self.wfile.write(bytes('0\r\n\r\n', 'utf-8'))

    def get_chunked_msg(self):
        chunk_size = os.read(self.rfile.fileno(), 2)
        while chunk_size[-2:] != b"\r\n":
            chunk_size += os.read(self.rfile.fileno(), 1)
        chunk_size = int(chunk_size[:-2], 16)

        chunk = os.read(self.rfile.fileno(), chunk_size)
        os.read(self.rfile.fileno(), 2)

        records = decoder.decode(chunk)

        if records:
            return records[0]

    def incref(self, container_id):
        containers[container_id]["lock"].acquire()
        containers[container_id]["refcount"] += 1
        containers[container_id]["lock"].release()

    def decref(self, container_id):
        containers[container_id]["lock"].acquire()
        containers[container_id]["refcount"] -= 1
        if containers[container_id]["refcount"] <= 0:
            del containers[container_id]
        else:
            containers[container_id]["lock"].release()
    

if __name__ == '__main__':
    print("Serving at port", PORT)
    server = StreamingTCPServer(("", PORT), StreamingRequestHandler)
    server.serve_forever()
