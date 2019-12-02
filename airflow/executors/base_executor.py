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
Base executor - this is the base class for all the implemented executors.
"""
from abc import ABCMeta
from collections import OrderedDict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

from airflow import LoggingMixin, conf
from airflow.models import TaskInstance
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstanceKeyType
from airflow.stats import Stats
from airflow.utils.state import State

PARALLELISM: int = conf.getint('core', 'PARALLELISM')

NOT_STARTED_MESSAGE = "The executor should be started first!"

# Command to execute - might be either string or list of strings
# with the same semantics as subprocess.Popen
CommandType = Union[str, List[str]]


# Task that is queued. It contains all the information that is
# needed to run the task.
#
# Tuple of: command, priority, queue name, SimpleTaskInstance
QueuedTaskInstanceType = Tuple[CommandType, int, Optional[str], SimpleTaskInstance]


class BaseExecutorProtocol(LoggingMixin):
    """
    Base Protocol implemented by all executors including multiple executors.
    """

    def __init__(self):
        super().__init__()

    def start(self):  # pragma: no cover
        """
        Executors may need to get things started.
        """
        raise NotImplementedError()

    def has_task(self, task_instance: TaskInstance) -> bool:
        """
        Checks if a task is either queued or running in this executor.

        :param task_instance: TaskInstance
        :return: True if the task is known to this executor
        """
        raise NotImplementedError()

    def sync(self) -> None:
        """
        Sync will get called periodically by the heartbeat method.
        Executors should override this to perform gather statuses.
        """
        raise NotImplementedError()

    def queue_command(self,
                      simple_task_instance: SimpleTaskInstance,
                      command: CommandType,
                      priority: int = 1,
                      queue: Optional[str] = None) -> None:
        """Queues command to task"""
        raise NotImplementedError()

    def queue_task_instance(self,
                            task_instance: TaskInstance,
                            mark_success: bool = False,
                            pickle_id: Optional[str] = None,
                            ignore_all_deps: bool = False,
                            ignore_depends_on_past: bool = False,
                            ignore_task_deps: bool = False,
                            ignore_ti_state: bool = False,
                            pool: Optional[str] = None,
                            cfg_path: Optional[str] = None) -> None:
        """Queues task instance."""
        raise NotImplementedError()

    def heartbeat(self) -> None:
        """
        Heartbeat sent to trigger new jobs.
        """
        raise NotImplementedError()

    def execute_async(self,
                      key: TaskInstanceKeyType,
                      command: CommandType,
                      queue: Optional[str] = None,
                      executor_config: Optional[Any] = None) -> None:  # pragma: no cover
        """
        This method will execute the command asynchronously.

        :param key: Unique key for the task instance
        :param command: Command to run
        :param queue: name of the queue
        :param executor_config: Configuration passed to the executor.
        """
        raise NotImplementedError()

    def end(self) -> None:  # pragma: no cover
        """
        This method is called when the caller is done submitting job and
        wants to wait synchronously for the job submitted previously to be
        all done.
        """
        raise NotImplementedError()

    def terminate(self):
        """
        This method is called when the daemon receives a SIGTERM
        """
        raise NotImplementedError()

    def is_task_queued(self, task_instance_key: TaskInstanceKeyType) -> bool:
        """
        Return True if task instance is queued
        """
        raise NotImplementedError()

    def is_task_running(self, task_instance_key: TaskInstanceKeyType) -> bool:
        """
        Return True if task instance is running
        """
        raise NotImplementedError()

    @property
    def queued_tasks_keys(self) -> Iterable[TaskInstanceKeyType]:
        """
        Returns task keys in iterable form.
        """
        raise NotImplementedError()

    def get_event_buffer(self, dag_ids=None) -> Dict[TaskInstanceKeyType, Optional[str]]:
        """
        Returns and flush the event buffer. In case dag_ids is specified
        it will only return and flush events for the given dag_ids. Otherwise
        it returns and flushes all events.

        :param dag_ids: to dag_ids to return events for, if None returns all
        :return: a dict of events
        """
        raise NotImplementedError()


class BaseExecutor(BaseExecutorProtocol, metaclass=ABCMeta):
    """
    Class to derive in order to interface with executor-type systems
    like Celery, Kubernetes, Local, Sequential and the likes.

    :param parallelism: how many jobs should run at one time. Set to
        ``0`` for infinity
    """
    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__()
        self._parallelism: int = parallelism
        self._queued_tasks: OrderedDict[TaskInstanceKeyType, QueuedTaskInstanceType] = OrderedDict()
        self._running: Set[TaskInstanceKeyType] = set()
        self._event_buffer: Dict[TaskInstanceKeyType, Optional[str]] = {}

    def has_task(self, task_instance: TaskInstance) -> bool:
        """
        Checks if a task is either queued or running in this executor.

        :param task_instance: TaskInstance
        :return: True if the task is known to this executor
        """
        return task_instance.key in self._queued_tasks or task_instance.key in self._running

    def queue_command(self,
                      simple_task_instance: SimpleTaskInstance,
                      command: CommandType,
                      priority: int = 1,
                      queue: Optional[str] = None):
        """Queues command to task"""
        if simple_task_instance.key not in self._queued_tasks \
                and simple_task_instance.key not in self._running:
            self.log.info("Adding to queue: %s", command)
            self._queued_tasks[simple_task_instance.key] = (command, priority, queue, simple_task_instance)
        else:
            self.log.info("could not queue task %s", simple_task_instance.key)

    def queue_task_instance(
            self,
            task_instance: TaskInstance,
            mark_success: bool = False,
            pickle_id: Optional[str] = None,
            ignore_all_deps: bool = False,
            ignore_depends_on_past: bool = False,
            ignore_task_deps: bool = False,
            ignore_ti_state: bool = False,
            pool: Optional[str] = None,
            cfg_path: Optional[str] = None):
        """Queues task instance."""
        pool = pool or task_instance.pool

        # TODO (edgarRd): AIRFLOW-1985:
        # cfg_path is needed to propagate the config values if using impersonation
        # (run_as_user), given that there are different code paths running tasks.
        # For a long term solution we need to address AIRFLOW-1986
        command_list_to_run = task_instance.command_as_list(
            local=True,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            pool=pool,
            pickle_id=pickle_id,
            cfg_path=cfg_path)
        self.queue_command(
            SimpleTaskInstance(task_instance),
            command_list_to_run,
            priority=task_instance.task.priority_weight_total,
            queue=task_instance.task.queue)

    def heartbeat(self) -> None:
        """
        Heartbeat sent to trigger new jobs.
        """
        if not self._parallelism:
            open_slots = len(self._queued_tasks)
        else:
            open_slots = self._parallelism - len(self._running)

        num_running_tasks = len(self._running)
        num_queued_tasks = len(self._queued_tasks)

        self.log.debug("%s running task instances", num_running_tasks)
        self.log.debug("%s in queue", num_queued_tasks)
        self.log.debug("%s open slots", open_slots)

        Stats.gauge('executor.open_slots', open_slots)
        Stats.gauge('executor._queued_tasks', num_queued_tasks)
        Stats.gauge('executor.running_tasks', num_running_tasks)

        self.trigger_tasks(open_slots)

        # Calling child class sync method
        self.log.debug("Calling the %s sync method", self.__class__)
        self.sync()

    def trigger_tasks(self, open_slots: int) -> None:
        """
        Triggers tasks

        :param open_slots: Number of open slots
        """
        sorted_queue = sorted(
            [(k, v) for k, v in self._queued_tasks.items()],
            key=lambda x: x[1][1],
            reverse=True)
        for _ in range(min((open_slots, len(self._queued_tasks)))):
            key, (command, _, _, simple_ti) = sorted_queue.pop(0)
            self._queued_tasks.pop(key)
            self._running.add(key)
            self.execute_async(key=key,
                               command=command,
                               queue=None,
                               executor_config=simple_ti.executor_config)

    def change_state(self, key: TaskInstanceKeyType, state: str) -> None:
        """
        Changes state of the task.

        :param key: Unique key for the task instance
        :param state: State to set for the task.
        """
        self.log.debug("Changing state: %s", key)
        try:
            self._running.remove(key)
        except KeyError:
            self.log.debug('Could not find key: %s', str(key))
        self._event_buffer[key] = state

    def fail(self, key: TaskInstanceKeyType) -> None:
        """
        Set fail state for the event.

        :param key: Unique key for the task instance
        """
        self.change_state(key, State.FAILED)

    def success(self, key: TaskInstanceKeyType) -> None:
        """
        Set success state for the event.

        :param key: Unique key for the task instance
        """
        self.change_state(key, State.SUCCESS)

    def get_event_buffer(self, dag_ids=None) -> Dict[TaskInstanceKeyType, Optional[str]]:
        """
        Returns and flush the event buffer. In case dag_ids is specified
        it will only return and flush events for the given dag_ids. Otherwise
        it returns and flushes all events.

        :param dag_ids: to dag_ids to return events for, if None returns all
        :return: a dict of events
        """
        cleared_events: Dict[TaskInstanceKeyType, Optional[str]] = dict()
        if dag_ids is None:
            cleared_events = self._event_buffer
            self._event_buffer = dict()
        else:
            for key in list(self._event_buffer.keys()):
                dag_id, _, _, _ = key
                if dag_id in dag_ids:
                    cleared_events[key] = self._event_buffer.pop(key)

        return cleared_events

    def is_task_queued(self, task_instance_key: TaskInstanceKeyType) -> bool:
        """
        Return True if task instance is queued
        """
        return task_instance_key in self._queued_tasks

    def is_task_running(self, task_instance_key: TaskInstanceKeyType) -> bool:
        """
        Return True if task instance is running
        """
        return task_instance_key in self._running

    @property
    def queued_tasks_keys(self) -> Iterable[TaskInstanceKeyType]:
        """
        Returns task keys in iterable form.
        """
        return self._queued_tasks.keys()

    @property
    def parallelism(self) -> int:
        """
        Parallelism level for the executor.
        """
        return self._parallelism
