import os
import time
import json
import psutil
from importlib import import_module
from collections import defaultdict
from pprint import pprint


"""
This is my implementation of the TaskExecutor. Each Task can be run at most:
max_instances (int) within a given time_bound (int: in minutes). 
Each Task's guard is dependent on it's task type.
E.g "TASK A" will have a particular max_instances and time_bound,
not necessarily the same as "TASK B"

That is how the guards are implemented, and it fits well within the logic of 
rate limiting as it's based on max_instances/time_bound.

Most of the methods are the standard getters/setters.

"""

# Managing process to retrieve tasks from queue and execute
class TaskExecutor:
    def __init__(self, task_queue, config):
        self.config = config
        self.task_queue = task_queue

        self.task_guards = config['task_guards']
        self.active_processes = {}
        self.task_counts = defaultdict(int)
        self.start_executor()

    """
    TODO: for global queue guards (update via postgres)
    Say you want to update max instances, time bound for a task_type.
    This might be because you just launched multiple instances of this service. Thus to not break the 
    functionality of the queue guards, you need to have some consensus on max instances, time bound
    outside the confines of any one service.

    Disclaimer, this might not be the right approach.
    """
    def update_all_queue_globals(self):
        pass
    
    # update max instances, time bound
    def update_global_guard_info(self, task_type, 
        max_instances=None, time_bound=None):
        if max_instances is not None:
            self.task_guards[task_type]['max_instances'] = max_instances
        if time_bound is not None:
            self.task_guards[task_type]['time_bound'] = time_bound

    # update prev time, for queue guards
    def update_local_guard_info(self, task_type, prev_time=None):
        if prev_time is not None:
            self.task_guards[task_type]['prev_time'] = prev_time

    # checks if a guard has been exceeded, given max_instances for a particular task type.
    def guard_exceeded(self, task_type):
        max_instances = self.task_guards[task_type]['max_instances']
        #print('TASK:', task_type, 'COUNTS:', self.task_counts[task_type])
        return self.task_counts[task_type] >= max_instances

    def get_all_task_types(self):
        return tuple(self.task_guards.keys())

    def increment_task_count(self, task_type):
        self.task_counts[task_type] += 1

    def decrement_task_count(self, task_type):
        self.task_counts[task_type] -= 1
    
    def set_task_count(self, task_type, task_count):
        self.task_counts[task_type] = task_count

    # getter for # of tasks for a particular type
    def get_task_count(self, task_type):
        return self.task_counts[task_type]

    # num of how many tasks of a given type are active
    def count_active_tasks_of_type(self, task_type):
        active_count = 0
        all_pids = psutil.pids()

        for pid in self.active_processes.keys():
            if pid in all_pids:
                active_count += 1

        return active_count

    # num of all active tasks. uses count_active_tasks_of_type() as a helper
    def total_active_tasks(self):
        total_tasks_num = 0

        for task_type in self.get_all_task_types():
            tasks_of_type = self.count_active_tasks_of_type(task_type)
            total_tasks_num += tasks_of_type

        return total_tasks_num

    """ 
    Helper method for try_refresh_guard().
    Checks if it's time for a particular task type to get it's task count reset.
    Essentially checks if it's possible to free the guard. 
    """
    def is_task_count_refresh_window(self, task_type):
        guard_info = self.task_guards[task_type]
        time_bound = guard_info['time_bound']
        prev_time = guard_info['prev_time']

        if time.time() - prev_time >= time_bound:
            return True
        else:
            return False

    """
    tries to refresh a guard for a particular type, if it's time to do so.
    If there are tasks still executing during the refresh window, they carry over.
    This ensures we can't go over our rate limit for a particular task type, 
    even with a fresh guard.
    """
    def try_refresh_guard(self, task_type):
        if self.is_task_count_refresh_window(task_type):
            print('REFRESHING GUARD:', task_type)
            active_tasks = self.count_active_tasks_of_type(task_type)
            self.set_task_count(task_type, active_tasks)
            self.update_local_guard_info(task_type, 
                prev_time=time.time())

    """
    Validates whether a given task still lives. If not, then we kill it, and
    remove it from active_processes.
    """
    def cleanup_finished_processes(self):
        pids_to_rm = []
        if len(self.active_processes) == 0:
            return

        for pid, task in self.active_processes.items():
            if not task.proc.is_alive():
                print('TASK FINISHED:', pid, task.task_type)
                pids_to_rm.append(pid)

        for pid in pids_to_rm:
            self.active_processes.pop(pid, None)

    """ 
    Starts the continuous execution of tasks on the queue.
    This Executor operates with conservative efficiency. 
    e.g It'll make sure that it's really really hard to break a task's guards,
    and if it thinks a task is ready to go, it'll spin it up ASAP.
    """
    def start_executor(self):
        while True:
            #print('-')
            # TODO: retrieve any new global info on your guards
            self.update_all_queue_globals()

            # check on active tasks/processes first
            self.cleanup_finished_processes()           

            if self.total_active_tasks() >= self.config['max_threads']:
                time.sleep(1)
                continue

            # greedily pop next task, even if's its not ready
            self.task_queue.sort_by_priority()

            status, next_task = self.task_queue.get_next_ready_task()
            if status == 'error':
                self.task_queue.readd_task(next_task)
                continue

            next_type = next_task.task_type
            next_id = next_task.task_id

            # refresh guards if ready, or otherwise check them
            self.try_refresh_guard(next_type)

            # we refresh first, bc otherwise it hangs forever once a guard is exceeded
            if self.guard_exceeded(next_type):
                print("GUARD TRIGGERED:", next_type)
                self.task_queue.readd_task(next_task)
                continue

            # start next task if it's kosher e.g the guard is not exceeded
            task_pid = next_task.execute()
            print('NEXT TASK:', next_type, 'PID:', task_pid)
            self.active_processes[task_pid] = next_task
            self.increment_task_count(next_type)

if __name__ == '__main__':
    pass
