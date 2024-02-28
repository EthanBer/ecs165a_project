import threading
import time

def target1(): # longer
    time.sleep(0.1)
    #print("target1 running")
    time.sleep(4)

def target2(): # shorter
    time.sleep(0.1)
    #print("target2 running")
    time.sleep(2)

def launch_thread_with_message(target, message, args=[], kwargs={}):
    def target_with_msg(*args, **kwargs):
        target(*args, **kwargs)
        #print(message)
    thread = threading.Thread(target=target_with_msg, args=args, kwargs=kwargs)
    thread.start()
    return thread

if __name__ == '__main__':
    thread1 = launch_thread_with_message(target1, change_page_directory())
    thread2 = launch_thread_with_message(target2, "finished target2")

    #print("main: launched all threads")

    thread1.join()
    thread2.join()

    #print("main: finished all threads")
