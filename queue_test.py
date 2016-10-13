#!/usr/bin/env python3
import sys
import os
import time
import multiprocessing
import pickle
import zmq
import traceback
import posix_ipc
import sysv_ipc
import threading

from multiprocessing.heap import BufferWrapper

# Monkeypatch for: https://bugs.python.org/issue28053
from multiprocessing.connection import _ConnectionBase
from multiprocessing.reduction import ForkingPickler
def _conn_base_send(self, obj):
    self._check_closed()
    self._check_writable()
    self._send_bytes(ForkingPickler.dumps(obj, pickle.HIGHEST_PROTOCOL))

_ConnectionBase.send = _conn_base_send

try:
    import nanomsg
    ENABLE_NANO = True
except:
    ENABLE_NANO = False

doc = {
    'something': 'More' * 50,
    'another': 'thing',
    'what?': range(200),
    'ok': ['asdf', 'asdf', 'asdf']
}

num_messages = 250000
zmq_socket_in = "ipc:///tmp/zmq_test_in"
zmq_socket_out = "ipc:///tmp/zmq_test_out"
nano_socket_out = "ipc:///tmp/nano_test_out"
nano_socket_in = "ipc:///tmp/nano_test_in"
ipc_name_out = 'ipc_test_out'
ipc_name_in = 'ipc_test_in'

sysv_ipc_key_out = 1
sysv_ipc_key_in = 1

def print_exc(func):
    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            traceback.print_exc()
            raise

    return func_wrapper

@print_exc
def posix_ipc_worker(rdy_evt: multiprocessing.Event):
    mq_in = posix_ipc.MessageQueue(ipc_name_out)
    mq_out = posix_ipc.MessageQueue(ipc_name_in)

    rdy_evt.set()

    try:
        size_messages = 0
        for task_nbr in range(num_messages):
            message, _ = mq_in.receive()
            message = pickle.loads(message)

            # pickle back to send pyobj
            message = pickle.dumps(message, pickle.HIGHEST_PROTOCOL)
            mq_out.send(message)
            size_messages += len(message)
    finally:
        mq_in.close()

    print("Total size:", size_messages)


@print_exc
def sysv_ipc_worker(rdy_evt: multiprocessing.Event):
    mq_in = sysv_ipc.MessageQueue(sysv_ipc_key_out)
    mq_out = sysv_ipc.MessageQueue(sysv_ipc_key_in)

    rdy_evt.set()

    try:
        size_messages = 0
        for task_nbr in range(num_messages):
            message, _ = mq_in.receive()
            message = pickle.loads(message)

            # pickle back to send pyobj
            message = pickle.dumps(message, pickle.HIGHEST_PROTOCOL)
            mq_out.send(message)
            size_messages += len(message)
    finally:
        mq_in.remove()
        mq_out.remove()
        pass

    print("Total size:", size_messages)

@print_exc
def queue_worker(q_in, q_out, rdy_evt: multiprocessing.Event):
    size_messages = 0
    is_joinable = q_in.__class__ == multiprocessing.JoinableQueue().__class__

    rdy_evt.set()

    for task_nbr in range(num_messages):
        message = q_in.get()
        if is_joinable:
            q_in.task_done()

        q_out.put(message)
        size_messages += len(message)

    print("Total size:", size_messages)


@print_exc
def pipe_worker(q_in, q_out, rdy_evt: multiprocessing.Event):
    size_messages = 0
    rdy_evt.set()

    for task_nbr in range(num_messages):
        message = q_in.recv()
        q_out.send(message)

        size_messages += len(message)

    print("Total size:", size_messages)

@print_exc
def zmq_worker(rdy_evt: multiprocessing.Event):
    context = zmq.Context()
    work_receiver = context.socket(zmq.PULL)
    work_receiver.connect(zmq_socket_out)

    work_sender = context.socket(zmq.PUSH)
    work_sender.connect(zmq_socket_in)

    rdy_evt.set()

    size_messages = 0
    for task_nbr in range(num_messages):
        message = work_receiver.recv_pyobj()
        work_sender.send_pyobj(message, protocol=pickle.HIGHEST_PROTOCOL)
        size_messages += len(message)

    print("Total size:", size_messages)


@print_exc
def nano_worker(rdy_evt: multiprocessing.Event):
    socket_in = nanomsg.Socket(nanomsg.PULL)
    socket_in.connect(nano_socket_out)

    socket_out = nanomsg.Socket(nanomsg.PUSH)
    socket_out.connect(nano_socket_in)

    rdy_evt.set()

    size_messages = 0
    for task_nbr in range(num_messages):
        message = socket_in.recv()
        message = pickle.loads(message)

        # we pickle back to simulate sending a python object
        message = pickle.dumps(message, pickle.HIGHEST_PROTOCOL)
        socket_out.send(message)

        size_messages += len(message)

    print("Total size:", size_messages)


def wait_on_sender_receiver(sender, receiver, proc_method, *proc_args):
    rdy_evt = multiprocessing.Event()
    proc = multiprocessing.Process(target=proc_method, args=proc_args + tuple([rdy_evt]))
    proc.start()
    rdy_evt.wait()

    t1 = threading.Thread(target=sender)
    t2 = threading.Thread(target=receiver)

    start_time = time.time()
    t1.start()
    t2.start()

    t2.join()  # wait for receiver to finish
    end_time = time.time()

    t1.join()
    proc.join()

    duration = end_time - start_time
    msg_per_sec = num_messages / duration

    print("Duration: %s" % duration)
    print("Messages Per Second: %s" % msg_per_sec + os.linesep)

def posix_ipc_test():
    print("Posix IPC")
    mq_out = posix_ipc.MessageQueue(ipc_name_out, flags=os.O_CREAT | os.O_EXCL)
    mq_in = posix_ipc.MessageQueue(ipc_name_in, flags=os.O_CREAT | os.O_EXCL)

    try:
        def sender():
            for num in range(num_messages):
                message = pickle.dumps(doc, pickle.HIGHEST_PROTOCOL)
                mq_out.send(message)

        def receiver():
            for num in range(num_messages):
                message, _ = mq_in.receive()
                message = pickle.loads(message)

        wait_on_sender_receiver(sender, receiver, posix_ipc_worker)
    finally:
        mq_out.close()
        mq_in.close()
        mq_out.unlink()
        mq_in.unlink()


def sysv_ipc_test():
    print("SYSV IPC")

    try:
        mq_out = sysv_ipc.MessageQueue(sysv_ipc_key_out, flags=os.O_CREAT | os.O_EXCL)
        mq_in = sysv_ipc.MessageQueue(sysv_ipc_key_in, flags=os.O_CREAT | os.O_EXCL)

        @print_exc
        def sender():
            for num in range(num_messages):
                message = pickle.dumps(doc, pickle.HIGHEST_PROTOCOL)
                mq_out.send(message)

        @print_exc
        def receiver():
            for num in range(num_messages):
                message, _ = mq_in.receive()
                message = pickle.loads(message)

        wait_on_sender_receiver(sender, receiver, sysv_ipc_worker)
    finally:
        # mq.remove()
        pass


# def mp_buf_test():
#     print("Multiprocessing Buffer")
#     proc = multiprocessing.Process(target=sysv_ipc_worker, args=())
#     proc.start()
#     time.sleep(2)
#
#     start_time = time.time()
#     for num in range(num_messages):
#         message = pickle.dumps(doc)
#         mq.send(message)
#
#     proc.join()


def zmq_test():
    print("ZMQ")

    files = [p.replace('ipc://', '') for p in [zmq_socket_in, zmq_socket_out]]
    for f in files:
        if os.path.exists(f): os.unlink(f)

    context = zmq.Context()
    socket_out = context.socket(zmq.PUSH)
    socket_out.bind(zmq_socket_out)

    socket_in = context.socket(zmq.PULL)
    socket_in.bind(zmq_socket_in)

    @print_exc
    def sender():
        for num in range(num_messages):
            socket_out.send_pyobj(doc, protocol=pickle.HIGHEST_PROTOCOL)

    @print_exc
    def receiver():
        for num in range(num_messages):
            socket_in.recv_pyobj()

    wait_on_sender_receiver(sender, receiver, zmq_worker)


def nano_test():
    print("Nano")

    files = [p.replace('ipc://', '') for p in [nano_socket_in, nano_socket_out]]
    for f in files:
        if os.path.exists(f): os.unlink(f)

    # strangely with nano the connect needs to happen before the bind
    @print_exc
    def sender():
        socket_out = nanomsg.Socket(nanomsg.PUSH)
        socket_out.bind(nano_socket_out)

        for num in range(num_messages):
            message = pickle.dumps(doc, pickle.HIGHEST_PROTOCOL)
            socket_out.send(message)

    @print_exc
    def receiver():
        socket_in = nanomsg.Socket(nanomsg.PULL)
        socket_in.bind(nano_socket_in)

        for num in range(num_messages):
            message = socket_in.recv()
            message = pickle.loads(message)

    wait_on_sender_receiver(sender, receiver, nano_worker)


def queue_test():
    print("MP Queue")
    send_q = multiprocessing.Queue()
    recv_q = multiprocessing.Queue()

    @print_exc
    def sender():
        for num in range(num_messages):
            send_q.put(doc)

    @print_exc
    def receiver():
        for num in range(num_messages):
            recv_q.get()

    wait_on_sender_receiver(sender, receiver, queue_worker, send_q, recv_q)


def joinable_queue_test():
    print("Joinable Queue")
    send_q = multiprocessing.JoinableQueue()
    recv_q = multiprocessing.JoinableQueue()

    @print_exc
    def sender():
        for num in range(num_messages):
            send_q.put(doc)

    @print_exc
    def receiver():
        for num in range(num_messages):
            recv_q.get()

    wait_on_sender_receiver(sender, receiver, queue_worker, send_q, recv_q)
    send_q.join()


def pipe_test():
    print("MP Pipe")
    receive_conn, send_conn = multiprocessing.Pipe(False)
    receive_conn2, send_conn2 = multiprocessing.Pipe(False)

    @print_exc
    def sender():
        for num in range(num_messages):
            send_conn.send(doc)

    @print_exc
    def receiver():
        for num in range(num_messages):
            receive_conn2.recv()

    wait_on_sender_receiver(sender, receiver, pipe_worker, receive_conn, send_conn2)


def main():
    # sysv_ipc_test()
    posix_ipc_test()
    queue_test()
    joinable_queue_test()
    pipe_test()
    zmq_test()

    if ENABLE_NANO:
        nano_test()

if __name__ == "__main__":
    main()
