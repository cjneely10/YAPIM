import threading
import unittest
from time import sleep

from HPCBioPipe.tasks.utils.thread_pool_queue import ThreadPoolQueue


class TestThreadPoolQueue(unittest.TestCase):
    def test_something(self):
        def run(n):
            print(n)
            sleep(0.4)
            print(n)

        q = ThreadPoolQueue(2)
        q.submit(run, 1)
        q.submit(run, 2)
        q.submit(run, 3)
        q.submit(run, 4)
        q.submit(run, 5)
        q.start()
        q.join()


if __name__ == '__main__':
    unittest.main()

