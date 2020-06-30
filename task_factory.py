import uuid
import multiprocessing as mp

""" 
Fairly self evident, wrapper for Task constructor.
"""

class TaskFactory:
    def __init__(self, task_refs):
        self.task_refs = task_refs

    # custom id, but feel free to change this to whatever
    def __generate_task_id(self):
        return uuid.uuid4()

    # get function call for a given task type
    def __get_task_reference(self, task_type):
        return self.task_refs[task_type]

    def new_task(self, task_type, task_data):
        task_ref = self.__get_task_reference(task_type)
        task_id = self.__generate_task_id()
        return Task(task_ref, task_type, task_id, task_data)

# Task class containing task status, metadata, and execution context
class Task:
    def __init__(self, task_ref, task_type, task_id, task_data):
        self.task_ref = task_ref
        self.task_type = task_type
        self.task_id = task_id

        self.task_data = task_data
        self.priority = 100 # TODO: this will probably be a part of task_data
        self.status = 'ready'
        self.metadata = None


        self.pid = None
        self.proc = None

    def __start_proc(self):
        args = (self.task_id, self.task_data)
        self.proc = mp.Process(target=self.task_ref, args=args)
        self.proc.daemon = True 
        self.proc.start()

        # ensure the process has started before we set the pid
        while self.proc.pid is None:
            pass
        self.pid = self.proc.pid
        
    # TODO: Create new process and return process ID, then somehow handle response
    def execute(self):
        self.status = 'executing'
        self.__start_proc()
        return self.pid