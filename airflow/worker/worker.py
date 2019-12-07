# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
This module contains task consumer and workers.
"""

import os
import signal
import subprocess
import sys
from multiprocessing import Process
from time import sleep
from typing import Any, Callable, Dict, List, Tuple

import psutil
import setproctitle
from kombu import Connection, Consumer, Message, Queue
from kombu.mixins import ConsumerMixin

from airflow import conf
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.worker.exchange import task_exchange

AIRFLOW_WORKER_NAME = "airflow-worker"


def add_signal_handler(func: Callable) -> None:
    """
    Register a SIGINT and SIGTERM handler.
    """
    signal.signal(signal.SIGINT, func)
    signal.signal(signal.SIGTERM, func)


class TaskConsumer(ConsumerMixin, LoggingMixin):
    """
    Task consumer.
    """
    def __init__(self, connection: Connection, queues: List[str]) -> None:
        self.connection = connection
        self.queues = [Queue(q, task_exchange, routing_key=q) for q in queues]

    def get_consumers(self, consumer, channel) -> List[Consumer]:
        return [
            consumer(q, callbacks=[self.run_task], accept=["json"])
            for q in self.queues
        ]

    def run_task(self, body: Dict[Any, Any], message: Message) -> None:
        """
        Executes ``airflow tasks run`` command.

        :param body: Body of the received message
        :param message: Message instance
        :return:
        """
        if "cmd" in body:
            env = os.environ.copy()
            subprocess.check_call(
                body["cmd"], stderr=subprocess.STDOUT, close_fds=True, env=env
            )
        message.ack()

    def stop(self) -> None:
        """
        When this is set to true the consumer should stop consuming
        and return, so that it can be joined if it is the implementation
        of a thread.
        """
        self.should_stop = True


class TaskWorker(LoggingMixin):
    """
    Single instance of task worker that runs a single TaskConsumer.
    The worker kills gracefully on SIGTERM and SIGINT.
    """
    def __init__(self, name: str = "airflow-task-worker") -> None:
        self.name = name
        self.amq_url = conf.get("worker", "broker_url")
        self.queues = conf.get("worker", "queues").split(",")
        with Connection(self.amq_url) as conn:
            self.consumer = TaskConsumer(conn, queues=self.queues)

    def start(self) -> None:
        """
        Starts TaskConsumer.
        """
        # Add signal handler for TaskWorker, done here because it's a
        # starting point for new process
        add_signal_handler(self.stop)

        setproctitle.setproctitle(self.name)
        self.log.debug("Consuming task from %s on %s", self.queues, self.amq_url)
        self.consumer.run()

    def stop(self, signum, frame) -> None:  # pylint: disable=unused-argument
        """
        Stops TaskConsumer.
        """
        self.log.info("Stopping task worker: %s", self.name)
        self.consumer.stop()


class Worker(LoggingMixin):
    """
    This class implements an Airflow Worker that runs many TaskWorkers.
    The number of TaskWorkers can be configured in airflow.cfg by setting
    worker|concurrency number.

    :param concurrency: Number of TaskWorkers to spawn.
    :type concurrency: int
    """
    def __init__(self, concurrency: int):
        self.name = AIRFLOW_WORKER_NAME
        self.workers = [TaskWorker for _ in range(concurrency)]
        self.should_be_running = True
        self.processes: Dict[int, Tuple[Process, TaskWorker]] = {}

    def start(self) -> None:
        """
        Starts main loop that monitors TaskWorkers and tries to
        revive terminated workers.
        """
        # Add signal handlers for main worker
        add_signal_handler(self.exit_gracefully)

        # Set process name
        setproctitle.setproctitle(self.name)

        for wid, worker in enumerate(self.workers):
            app = worker(name=f"task-worker-{wid}")
            proc = Process(target=app.start, daemon=True)
            proc.start()
            self.processes[wid] = (proc, app)

        # The main worker loop
        while self.should_be_running:
            for wid, (proc, app) in self.processes.items():
                self.revive_process(wid, proc, app)
            sleep(0.5)

    def revive_process(self, wid: int, process: Process, app: TaskWorker) -> None:
        """
        Checks if process is still alive, if not then creates new one.
        """
        if not process.is_alive() and self.should_be_running:
            proc = Process(target=app.start, daemon=True)
            proc.start()
            self.processes[wid] = (proc, app)

    def exit_gracefully(self, signum, frame) -> None:  # pylint: disable=unused-argument
        """
        Provides graceful exit of the worker.
        """
        self.log.info("Gracefully stopping: %s", self.name)
        self.should_be_running = False
        sys.exit(0)

    @staticmethod
    def stop() -> None:
        """
        Sends SIGTERM signal to all Airflow workers on single machine.
        """
        for proc in psutil.process_iter():
            if proc.name() == AIRFLOW_WORKER_NAME:
                proc.send_signal(signal.SIGTERM)
