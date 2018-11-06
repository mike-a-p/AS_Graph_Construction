import sys
import os
import psutil

class progress_bar:
	
	def __init__(self, total_tasks, total_jobs = None):
		self.tasks_done = 0
		self.total_tasks = total_tasks
		self.progress = 0
		self.process = psutil.Process(os.getpid())
		self.bar = 0
		self.B_per_GB = (1024**3)
		self.increment = 1/40

		if(total_jobs):
			self.total_jobs = total_jobs
			self.current_job = 0
		else:
			self.total_jobs = 1
			self.current_job = 1
		jobs_done = str(self.current_job) + "/" + str(self.total_jobs)
		sys.stdout.write(jobs_done + " [" + " "*40 + "] ") 
		

	def update(self):
		self.tasks_done = self.tasks_done + 1
		if((self.tasks_done/self.total_tasks)-self.progress > self.increment):
			self.progress = self.tasks_done/self.total_tasks
			self.bar = int(self.progress/self.increment)
			memoryUse = self.process.memory_info()[0]
			memoryGB = memoryUse/(self.B_per_GB)
			jobs_done = str(self.current_job) + "/" + str(self.total_jobs)
			load = jobs_done + " ["+"-"*self.bar + " "*(39-self.bar) + "] " + str(memoryGB) + "GB "
			sys.stdout.write('\r' + load)

	def next_job(self, total_tasks):
		self.current_job = self.current_job + 1
		self.reset()
		self.total_tasks = total_tasks

	def reset(self):
		self.tasks_done = 0
		self.progress = 0
		self.bar = 0
#		sys.stdout.write("[" + " "*40 + "] ")

	def finish(self):
		print()
