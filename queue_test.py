import sys
import os
import time
import multiprocessing
import pickle
import zmq
import traceback
import posix_ipc
import platform

try:
    import nanomsg
    ENABLE_NANO = True
except:
    ENABLE_NANO = False

doc = {
    'something': 'More',
    'another': 'thing',
    'what?': range(200),
    'ok': ['asdf', 'asdf', 'asdf']
}

num_messages = 500000
zmq_socket = "ipc:///tmp/zmq_test"
nano_socket = "ipc:///tmp/nano_test"

if platform.system() == "Darwin":
    ipc_file = "/tmp/ipc_test"
else:
    ipc_file = "/ipc_test"  # needs to be a "root" path or else it will give permission denied


def ipc_worker():
    mq = posix_ipc.MessageQueue(ipc_file)
    try:
        size_messages = 0
        for task_nbr in range(num_messages):
            message, priority = mq.receive()
            message = pickle.loads(message)
            size_messages += len(message)
    finally:
        mq.close()

    print("Total size:", size_messages)
    sys.exit(1)


def queue_worker(q):
    size_messages = 0
    is_joinable = q.__class__ == multiprocessing.JoinableQueue().__class__

    for task_nbr in range(num_messages):
        message = q.get()
        if is_joinable:
            q.task_done()

        size_messages += len(message)

    print("Total size:", size_messages)
    sys.exit(1)


def pipe_worker(q):
    size_messages = 0
    for task_nbr in range(num_messages):
        message = q.recv()
        size_messages += len(message)

    print("Total size:", size_messages)
    sys.exit(1)


def zmq_worker():
    context = zmq.Context()
    work_receiver = context.socket(zmq.PULL)
    work_receiver.connect(zmq_socket)

    size_messages = 0
    for task_nbr in range(num_messages):
        message = work_receiver.recv()
        message = pickle.loads(message)
        size_messages += len(message)

    print("Total size:", size_messages)
    sys.exit(1)


def nano_worker():
    try:
        socket = nanomsg.Socket(nanomsg.PULL)
        socket.connect(nano_socket)

        size_messages = 0
        for task_nbr in range(num_messages):
            message = socket.recv()
            message = pickle.loads(message)
            size_messages += len(message)

        print("Total size:", size_messages)
    except:
        traceback.print_exc()

    sys.exit(1)


def ipc_test():
    print("IPC")
    if os.path.exists(ipc_file):
        os.unlink(ipc_file)

    mq = posix_ipc.MessageQueue(ipc_file, flags=os.O_CREAT|os.O_EXCL)
    try:
        proc = multiprocessing.Process(target=ipc_worker, args=())
        proc.start()
        time.sleep(2)

        start_time = time.time()
        for num in range(num_messages):
            message = pickle.dumps(doc)
            mq.send(message)

        proc.join()
    finally:
        mq.close()
        mq.unlink()

    end_time = time.time()
    duration = end_time - start_time
    msg_per_sec = num_messages / duration

    print("Duration: %s" % duration)
    print("Messages Per Second: %s" % msg_per_sec)


def zmq_test():
    print("ZMQ")
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind(zmq_socket)

    proc = multiprocessing.Process(target=zmq_worker, args=())
    proc.start()
    time.sleep(2)

    start_time = time.time()
    for num in range(num_messages):
        message = pickle.dumps(doc)
        socket.send(message)

    proc.join()

    end_time = time.time()
    duration = end_time - start_time
    msg_per_sec = num_messages / duration

    print("Duration: %s" % duration)
    print("Messages Per Second: %s" % msg_per_sec)


def nano_test():
    print("Nano")
    proc = multiprocessing.Process(target=nano_worker, args=())
    proc.start()

    # strangely the PULL has to connect before the PUSH
    socket = nanomsg.Socket(nanomsg.PUSH)
    socket.bind(nano_socket)

    start_time = time.time()
    for num in range(num_messages):
        message = pickle.dumps(doc)
        socket.send(message)
    proc.join()

    end_time = time.time()
    duration = end_time - start_time
    msg_per_sec = num_messages / duration

    print("Duration: %s" % duration)
    print("Messages Per Second: %s" % msg_per_sec)


def queue_test():
    print("MP Queue")
    send_q = multiprocessing.Queue()
    proc = multiprocessing.Process(target=queue_worker, args=(send_q,))
    proc.start()

    start_time = time.time()
    for num in range(num_messages):
        send_q.put(doc)

    proc.join()
    end_time = time.time()
    duration = end_time - start_time
    msg_per_sec = num_messages / duration

    print("Duration: %s" % duration)
    print("Messages Per Second: %s" % msg_per_sec)


def joinable_queue_test():
    print("Joinable Queue")
    send_q = multiprocessing.JoinableQueue()
    proc = multiprocessing.Process(target=queue_worker, args=(send_q,))
    proc.start()

    start_time = time.time()
    for num in range(num_messages):
        send_q.put(doc)

    proc.join()
    send_q.join()

    end_time = time.time()
    duration = end_time - start_time
    msg_per_sec = num_messages / duration

    print("Duration: %s" % duration)
    print("Messages Per Second: %s" % msg_per_sec)


def pipe_test():
    print("MP Pipe")
    receive_conn, send_conn = multiprocessing.Pipe(False)
    proc = multiprocessing.Process(target=pipe_worker, args=(receive_conn,))
    proc.start()

    start_time = time.time()
    for num in range(num_messages):
        send_conn.send(doc)

    proc.join()
    end_time = time.time()
    duration = end_time - start_time
    msg_per_sec = num_messages / duration

    print("Duration: %s" % duration)
    print("Messages Per Second: %s" % msg_per_sec)


def main():
    ipc_test()
    queue_test()
    joinable_queue_test()
    pipe_test()
    zmq_test()

    if ENABLE_NANO:
        nano_test()

if __name__ == "__main__":
    main()
