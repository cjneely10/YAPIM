import queue
import threading


class ThreadPoolQueue(queue.Queue):
    def __init__(self, num_workers: int):
        super().__init__(-1)
        self.num_workers = num_workers

    def start(self):
        for i in range(self.num_workers):
            threading.Thread(target=self.run, daemon=True).start()

    def run(self):
        while True:
            item = self.get()
            item[0](*item[1])
            self.task_done()

    def submit(self, method, *args):
        packaged_call = (method, args)
        self.put(packaged_call)
