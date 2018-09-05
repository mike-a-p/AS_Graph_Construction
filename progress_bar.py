import sys
import os
import psutil

class progress_bar:
	
	def __init__(self, total_tasks, increment = None):
		self.tasks_done = 0
		self.total_tasks = total_tasks
		self.progress = 0
		self.process = psutil.Process(os.getpid())
		self.bar = 0
		self.B_per_GB = (1024**3)
		if(increment is None):
			self.increment = 1/40
		else:
			self.increment = increment
		sys.stdout.write("[" + " "*40 + "] ")
	def update(self):
		self.tasks_done = self.tasks_done + 1
		if((self.tasks_done/self.total_tasks)-self.progress > self.increment):
			self.progress = self.tasks_done/self.total_tasks
			self.bar = self.bar + 1
			memoryUse = self.process.memory_info()[0]
			memoryGB = memoryUse/(self.B_per_GB)
			load = "["+"-"*self.bar + " "*(39-self.bar) + "] " + str(memoryGB) + "GB "
			sys.stdout.write('\r' + load)

	def reset(self):
		self.tasks_done = 0
		self.progress = 0
		self.bar = 0
		sys.stdout.write("[" + " "*40 + "] ")
