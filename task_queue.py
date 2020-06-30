import os
import json
from importlib import import_module
from pprint import pprint

# temporary 
#from .tasks.index import task_scripts

from task_factory import TaskFactory
from task_factory import Task

""" 
The implementation of this will be up to ya, but I can say that bc Postgres is ACID,
it would make sense to have a dirty bit somewhere in here
"""
def get_backlog():
    # lqt = get_last_queued_task() 
    # return get_all_unqueued_tasks(lqt)
    pass

"""
Here's our task queue. It behaves mostly as you'd expect. 
It uses the TaskFactory class to create new tasks, and has no internal housekeeping--
meaning it's state can only be modified by code that calls it's helper methods.
In our test environment, that's test.py. But in prod, web_server.py.
"""

# Task queue to manage ordering of tasks
class TaskQueue:
    def __init__(self, config):
        self.config = config
        self.queue = None
        self.construct_queue()
        self.task_factory = TaskFactory(config['tasks'])

    # sort the queue by priority if it makes sense to 
    def sort_by_priority(self):
        if len(self.queue) > 1:
            self.queue = sorted(self.queue, 
                key=lambda t: t.priority, reverse=True)

    # Constructs the task queue
    # Begins with initializing queue data structure, then retrieving backlogged tasks
    def construct_queue(self):
        self.queue = []

        backlog = get_backlog()
        if backlog is not None:
            self.queue.append(backlog)

        # log(queue_construced)
        # log(metadata_about_backlogged_tasks, etc)

    def get_all_task_ids(self):
        return (t.task_id for t in self.queue)

    # NOTE: All task operations return (op status, post-op task details)
    
    # Add a task to the queue: uses the task factory to create a new task.
    def add_task(self, task_type, task_data):
        new_task = self.task_factory.new_task(task_type, task_data)
        self.queue.append(new_task)
        return 'success', new_task

    # this is only used when a task that's been greedily popped off isn't ready yet
    # we check for none, because get_next_ready_task() may return None. 
    def readd_task(self, task):
        if task is not None:
            self.queue.append(task)

    # Return a task from the queue by ID
    def get_task(self, task_id):
        for task in self.queue:
            if task.task_id == task_id:
                return task

    # Removes a task from the queue by ID
    def remove_task(self, task_id):
        rm_idx = None
        for idx, task in enumerate(self.queue):
            if task.task_id == task_id:
                rm_idx = idx
                break

        self.queue.pop(rm_idx)
        return rm_idx is not None

    # Retrieves the next task in the queue that's ready for execution and marks it as being executed
    def get_next_ready_task(self):
        candidate_task = None
        candidate_task_status = None

        if len(self.queue) == 0:
            candidate_task = None
            print('No tasks in queue')
        else:
            candidate_task = self.queue.pop(0)

        status = 'success'
        if candidate_task is None:
            status = 'error'
        else:
            print('Successfully Popped:', candidate_task)

        return status, candidate_task

    # Modifies the properties of a task (for now just status)
    def modify_task(self, task_id, status):
        for task in self.queue:
            if task.task_id == task_id:
                task.status = status
                return task_id

if __name__ == '__main__':
    pass
