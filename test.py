
"""
This is the file I used to test my implementation of the executor and the queue.
Notice that I didn't actually import the tasks from the tasks folder. 
I was having issues with importing them, so I just hard coded them here.
"""

import time
import random
from task_queue import TaskQueue
from task_executor import TaskExecutor

def scrape_a(task_id, task_data):
	print('Scraping A')
	time.sleep(2)
	return {'status': 'success', 'data': None}

def scrape_b(task_id, task_data):
	print('Scraping B')
	time.sleep(4)
	return {'status': 'success', 'data': None}

def scrape_c(task_id, task_data):
	print('Scraping C')
	time.sleep(8)
	return {'status': 'success', 'data': None}

task_scripts = {
    'SCRAPE_A': scrape_a,
    'SCRAPE_B': scrape_b,
    'SCRAPE_C': scrape_c
}

task_guards = {
	'SCRAPE_A': {'max_instances': 3, 'time_bound': 10, 'prev_time': time.time()},
    'SCRAPE_B': {'max_instances': 3, 'time_bound': 10, 'prev_time': time.time()},
    'SCRAPE_C': {'max_instances': 3, 'time_bound': 10, 'prev_time': time.time()}
}

""" 
Sort of simulates reality by creating a fake queue, with a bunch of jobs lined up in a random order.
"""
def generate_random_queue(jobs, job_max):
	queue = []

	for job in jobs:
		num_job = random.randrange(job_max)
		queue.extend((job,)*num_job)
		
	random.shuffle(queue)
	return queue

"""
Push our random queue onto our TaskQueue, and let the TaskExecutor do it's stuff.
Please notice the inputs for the TaskQueue, and the TaskExecutor.
"""
def test_task_queue():
	tq_config = {'tasks': task_scripts}
	tq = TaskQueue(config=tq_config)
	random_queue = generate_random_queue(task_scripts.keys(), 30)
	print("QUEUE:", random_queue)
		
	for elem in random_queue:
		tq.add_task(elem, None)

	te_config = {'task_guards': task_guards, 'max_threads': 40}
	te = TaskExecutor(tq, config=te_config)

if __name__ == '__main__':
	test_task_queue()
	pass
