import queue
import threading


class ThreadPoolQueue(queue.Queue):
    def __init__(self, max_size: int):
        super().__init__(max_size)
        self.max_size: int = max_size

    def start(self):
        while True:
            item = self.get()
            item[0](*item[1])
            self.task_done()

    def submit(self, method, *args):
        packaged_call = (method, args)
        self.put(packaged_call)
