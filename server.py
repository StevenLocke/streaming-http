#! /usr/bin/env python2.7

import os
import sys
import time
import BaseHTTPServer
import pickle
import socket
import SocketServer
import subprocess
import threading

from functools import partial

import agent_pb2 as pba
import mesos_pb2 as pbm


PORT = 8888

containers = {}

class StreamingTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)


class StreamingRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler, object):
    def __init__(self, request, client_address, server):
        # self.timeout = 0.1
        super(StreamingRequestHandler, self).__init__(
            request,
            client_address,
            server)

    def do_POST(self):
        #debugger
        import pdb; pdb.set_trace()

        if 'content-length' in self.headers:
            length = int(self.headers['content-length'])
            data = self.rfile.read(length)
            msg = pickle.loads(data)
    
            # if msg.type == pba.Call.LIST_CONTAINERS:
            #     return self.list_containers(msg.msg)
    
            if msg.type == pba.Call.LAUNCH_NESTED_CONTAINER_SESSION:
                #debugger
                # import pdb; pdb.set_trace()
                return self.handle_launch_nested_container_session(msg.msg)
    
            if msg.type == pba.Call.ATTACH_CONTAINER_OUTPUT:
                #debugger
                # import pdb; pdb.set_trace()
                return self.handle_attach_container_output_stream(msg.msg)

        if msg.type == pba.Call.ATTACH_CONTAINER_INPUT:
            return self.handle_attach_container_input_stream()



    # def list_containers(self, msg):
    #     #debugger
    #     import pdb; pdb.set_trace()


    #     list_containers_response = ListContainersResponse(containers.keys())
    #     pickled_msg = pickle.dumps(list_containers_response)

    #     self.send_response(200)
    #     self.send_header('Content-type', 'application/x-protobuf')
    #     self.send_header('Content-length', len(pickled_msg))
    #     self.end_headers()
    #     self.wfile.write(pickled_msg)
        

    def handle_launch_nested_container_session(self, msg):
        if msg.launch_nested_container_session.container_id in containers.keys():
            self.send_response(409)
            return

        self.send_response(200)
        self.send_header('Content-type', 'application/x-protobuf')
        self.send_header('transfer-encoding', 'chunked')
        self.end_headers()

        stdin_pipe = os.pipe()
        stdout_pipe = os.pipe()
        stderr_pipe = os.pipe()

        process = subprocess.Popen(
            [msg.launch_nested_container_session.command] + msg.launch_nested_container_session.args,
            close_fds=True,
            env={},
            stdin=stdin_pipe[0],
            stdout=stdout_pipe[1],
            stderr=stderr_pipe[1])

        os.close(stdin_pipe[0])
        os.close(stdout_pipe[1])
        os.close(stderr_pipe[1])

        containers[msg.container_id] = {
            "cmd" : msg.launch_nested_container_session.command,
            "args" : msg.launch_nested_container_session.args,
            "stdin_pipe" : stdin_pipe,
            "stdout_pipe" : stdout_pipe,
            "stderr_pipe" : stderr_pipe,
            "process" : process,
            "refcount" : 0,
            "lock" : threading.Lock(),
            "exit_event" : threading.Event()
        }

        process.wait()

        self.incref(msg.launch_nested_container_session.container_id)
        containers[msg.launch_nested_container_session.container_id]["exit_event"].clear()
        containers[msg.launch_nested_container_session.container_id]["exit_event"].wait()
        self.decref(msg.launch_nested_container_session.container_id)

        self.send_chunked_terminator()

    def handle_attach_container_output_stream(self, msg):
        self.send_response(200)
        self.send_header('Content-type', 'application/x-protobuf')
        self.send_header('transfer-encoding', 'chunked')
        self.end_headers()

        container_id = msg.attach_container_output.container_id

        stdin_write = containers[container_id]["stdin_pipe"][1]
        stdout_read = containers[container_id]["stdout_pipe"][0]
        stderr_read = containers[container_id]["stderr_pipe"][0]

        if not msg.launch_nested_container_session.interactive:
            os.close(stdin_write)

        for chunk in iter(partial(os.read, stdout_read, 1024), ''):
            #debugger
            # import pdb; pdb.set_trace()
            io_msg = pbm.ProcessIO()
            io_msg.type = pbm.processIO.Type.DATA
            io_msg.data.type = pbm.processIO.data.Type.STDOUT
            io_msg.data.data = chunk

            self.send_chunked_msg(io_msg)

        for chunk in iter(partial(os.read, stderr_read, 1024), ''):
            io_msg = pbm.ProcessIO()
            io_msg.type = pbm.processIO.Type.DATA
            io_msg.data.type = pbm.processIO.data.Type.STDERR
            io_msg.data.data = chunk

            self.send_chunked_msg(io_msg)

        os.close(stdout_read)
        os.close(stderr_read)

        self.send_chunked_terminator()

        containers[container_id]["exit_event"].set()

    def handle_attach_container_input_stream(self):
        try:
            msg = self.get_chunked_msg()
        except Exception as exception:
            self.send_response(400)
            self.send_header('Content-type', 'application/x-protobuf')
            self.end_headers()
            return

        container_id = msg.attach_container_input.container_id

        stdin_write = containers[container_id]["stdin_pipe"][1]

        error_code = 200

        try:
            while True:
                msg = self.get_chunked_msg()

                if msg.attach_container_input.type == pbm.ProcessIO.Type.DATA and \
                msg.attach_container_input.process_io.data.type == pbm.ProcessIO.Data.Type.STDIN:
                    data = msg.attach_container_input.process_io.data
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
        pickled_msg = pickle.dumps(msg)

        chunk = '%X\r\n%s\r\n' % (len(pickled_msg), pickled_msg)
        self.wfile.write(chunk)

    def send_chunked_terminator(self):
        self.wfile.write('0\r\n\r\n')

    def get_chunked_msg(self):
        chunk_size = os.read(self.rfile.fileno(), 2)
        while chunk_size[-2:] != b"\r\n":
            chunk_size += os.read(self.rfile.fileno(), 1)
        chunk_size = int(chunk_size[:-2], 16)

        chunk = os.read(self.rfile.fileno(), chunk_size)
        os.read(self.rfile.fileno(), 2)

        return pickle.loads(chunk)

    def incref(self, container_id):
        containers[container_id]["lock"].acquire()
        containers[container_id]["refcount"] += 1
        containers[container_id]["lock"].release()

    def decref(self, container_id):
        containers[container_id]["lock"].acquire()
        containers[container_id]["refcount"] -= 1
        if containers[container_id]["refcount"] == 0:
            del containers[container_id]
        else:
            containers[container_id]["lock"].release()
    

if __name__ == '__main__':
    print "Serving at port", PORT
    server = StreamingTCPServer(("", PORT), StreamingRequestHandler)
    server.serve_forever()
