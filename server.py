#! /usr/bin/env python2.7

import os
import sys
import time
import BaseHTTPServer
import socket
import SocketServer
import subprocess
import threading
import json

from google.protobuf.json_format import MessageToJson, Parse

from functools import partial

import mesos.v1.agent.agent_pb2 as pba
import mesos.v1.mesos_pb2 as pbm

# from twitter.common import recordio
# print(sys.path)
import recordio

PORT = 8888

containers = {}
tasksToContainer = {"1":"a", "2":"b"}
encoder = recordio.Encoder(MessageToJson)
decoder = recordio.Decoder(Parse)


ROUTES = [
    ('/mesos/master/api/v1')
]

class StreamingTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)


class StreamingRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler, object):
    def __init__(self, request, client_address, server):
        super(StreamingRequestHandler, self).__init__(
            request,
            client_address,
            server)

    def do_GET(self):
        # print self.path
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
                    'container_id': {
                        'value': containerID }}]}

            state['frameworks'][0]['tasks'].append(task)

        jsonState = json.dumps(state)

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Content-length', len(jsonState))
        self.end_headers()
        self.wfile.write(jsonState)

    def do_POST(self):
        if self.path == "/slave/1/api/v1":
            self.post_api_v1()
        else:
            self.send_response(400)            

    def post_api_v1(self):
        if 'content-length' in self.headers:
            length = int(self.headers['content-length'])
            data = self.rfile.read(length)
            msg = pba.Call()
            Parse(data, msg)
    
            # if msg.type == pba.Call.LIST_CONTAINERS:
            #     return self.list_containers(msg)
    
            if msg.type == pba.Call.LAUNCH_NESTED_CONTAINER_SESSION:
                return self.handle_launch_nested_container_session(msg)
    
            if msg.type == pba.Call.ATTACH_CONTAINER_OUTPUT:
                return self.handle_attach_container_output_stream(msg)

        return self.handle_attach_container_input_stream()



    # def list_containers(self, msg):

    #     list_containers_response = ListContainersResponse(containers.keys())
    #     pickled_msg = pickle.dumps(list_containers_response)

    #     self.send_response(200)
    #     self.send_header('Content-type', 'application/x-protobuf')
    #     self.send_header('Content-length', len(pickled_msg))
    #     self.end_headers()
    #     self.wfile.write(pickled_msg)
        

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

        # if not msg.launch_nested_container_session.interactive:
        #     os.close(stdin_write)

        # import pdb; pdb.set_trace()

        for chunk in iter(partial(os.read, stdout_read, 1024), ''):
            io_msg = pba.ProcessIO()
            io_msg.type = pba.ProcessIO.Type.Value('DATA')
            io_msg.data.type = pba.ProcessIO.Data.Type.Value('STDOUT')
            io_msg.data.data = chunk
            io_msg = encoder.encode(io_msg)
            self.send_chunked_msg(io_msg)

            # r, w = os.pipe()
            # w = os.fdopen(w, 'wb')
            # writer = recordio.StringRecordWriter(w)
            # writer.write(MessageToJson(io_msg))
            # w.close()
            # out = os.read(r, 100000000)
            # print(out)
            # self.send_chunked_msg(out)

        for chunk in iter(partial(os.read, stderr_read, 1024), ''):
            io_msg = pba.ProcessIO()
            io_msg.type = pba.ProcessIO.Type.Value('DATA')
            io_msg.data.type = pba.ProcessIO.Data.Type.Value('STDERR')
            io_msg.data.data = chunk
            io_msg = encoder.encode(io_msg)
            self.send_chunked_msg(io_msg)

            # r, w = os.pipe()
            # w = os.fdopen(w, 'wb')
            # writer = recordio.StringRecordWriter(w)
            # writer.write(MessageToJson(io_msg))
            # w.close()
            # out = os.read(r, 100000000)
            # print(out.encode('utf-8'))
            # self.send_chunked_msg(out)

        os.close(stdout_read)
        os.close(stderr_read)

        self.send_chunked_terminator()

        containers[container_id]["exit_event"].set()
        self.decref(container_id)

    def handle_attach_container_input_stream(self):
        try:
            chunk = self.get_chunked_msg()
            # print(chunk)
            msg = pba.Call()
            Parse(chunk, msg)
        except Exception as exception:
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            return

        container_id = msg.attach_container_input.container_id.value

        stdin_write = containers[container_id]["stdin_pipe"][1]

        error_code = 200

        try:
            #TODO: Fix timeout
            # self.socket.settimeout(1)
            while True:
                try:
                    chunk = self.get_chunked_msg()
                except Exception as exception:
                    continue
                msg = pba.Call.AttachContainerInput()
                Parse(chunk, msg)

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
        chunk = '%X\r\n%s\r\n' % (len(msg), msg)
        self.wfile.write(chunk)

    def send_chunked_terminator(self):
        self.wfile.write('0\r\n\r\n')

    def get_chunked_msg(self):
        import pdb; pdb.set_trace()
        #decoder
        chunk_size = os.read(self.rfile.fileno(), 2)
        while chunk_size[-2:] != b"\r\n":
            chunk_size += os.read(self.rfile.fileno(), 1)
        chunk_size = int(chunk_size[:-2], 16)

        chunk = os.read(self.rfile.fileno(), chunk_size)
        os.read(self.rfile.fileno(), 2)

        record = decoder.decode(chunk.decode('utf-8'))

        if record:
            return record

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
    print "Serving at port", PORT
    server = StreamingTCPServer(("", PORT), StreamingRequestHandler)
    server.serve_forever()
