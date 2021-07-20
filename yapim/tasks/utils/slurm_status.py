"""Manage status of in-progress SLURM jobs"""

import threading
from datetime import datetime, timedelta

from plumbum import local


class SlurmStatus:
    """Queries squeue job information for user, periodically queries to update cached status. Will re-check c. 60s"""
    def __init__(self, user_id: str):
        self.lock = threading.Lock()
        self._user_id = user_id
        self._time_last_checked = None
        self._status_message = None

    def _set_status(self):
        self._time_last_checked = datetime.now()
        self._status_message = str(local["squeue"]["-u", self._user_id]())

    def update(self):
        """Update currently tracked info once a job has been launched"""
        with self.lock:
            self._set_status()

    def check_status(self, job_id: str) -> bool:
        with self.lock:
            if self._status_message is None:
                self._set_status()
            else:
                current_time = datetime.now()
                if current_time - self._time_last_checked > timedelta(seconds=60):
                    self._set_status()
            return job_id in self._status_message
