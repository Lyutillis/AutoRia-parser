import multiprocessing
import threading
import time



def add_to_queue(queue):
    for i in range(10):
        queue.put(i)
        time.sleep(0.5)


def output_queue(queue):
    counter = 40
    while counter > 0:
        elem = queue.get()
        print(elem)
        counter -= 1


if __name__=="__main__":
    queue = multiprocessing.Queue()
    thread = threading.Thread(
        target=output_queue,
        args=(queue,)
    )
    thread.start()
    processes = []
    for i in range(4):
        process = multiprocessing.Process(
            target=add_to_queue,
            args=(queue,)
        )
        process.start()
        processes.append(process)
    
    for process in processes:
        process.join()

    thread.join()
