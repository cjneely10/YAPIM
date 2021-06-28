import threading
from datetime import datetime, timedelta

from plumbum import local


class SlurmStatus:
    def __init__(self, user_id: str):
        self.lock = threading.Lock()
        self._user_id = user_id
        self._time_last_checked = None
        self._status_message = None

    def _set_status(self):
        with self.lock:
            self._time_last_checked = datetime.now()
            self._status_message = str(local["squeue"]["-u", self._user_id]())

    def update(self):
        self._set_status()

    def check_status(self, job_id: str) -> bool:
        if self._status_message is None:
            self._set_status()
        else:
            current_time = datetime.now()
            if current_time - self._time_last_checked > timedelta(minutes=1):
                self._set_status()
        return job_id in self._status_message
