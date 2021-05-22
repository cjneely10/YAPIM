import threading
import unittest
from time import sleep

from HPCBioPipe.tasks.utils.thread_pool_queue import ThreadPoolQueue


class TestThreadPoolQueue(unittest.TestCase):
    def test_something(self):
        def run(n):
            print(n)
            print(n)

        q = ThreadPoolQueue(3)
        threading.Thread(target=q.start, daemon=True).start()
        q.submit(run, 1)
        q.submit(run, 2)
        q.submit(run, 3)
        q.submit(run, 4)
        q.submit(run, 5)
        q.join()


if __name__ == '__main__':
    unittest.main()

