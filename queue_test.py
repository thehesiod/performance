import sys
import time
import multiprocessing
import pickle
import zmq
import nanomsg
import traceback


doc = {
    'something': 'More',
    'another': 'thing',
    'what?': range(200),
    'ok': ['asdf', 'asdf', 'asdf']
}

num_messages = 500000
zmq_socket = "ipc:///tmp/zmq_test"
nano_socket = "ipc:///tmp/nano_test"


def queue_worker(q):
    size_messages = 0
    for task_nbr in range(num_messages):
        message = q.get()
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
    queue_test()
    pipe_test()
    zmq_test()
    nano_test()

if __name__ == "__main__":
    main()
