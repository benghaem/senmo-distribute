import subprocess
import zmq
import time

class PortManager(object):
	"""Port manager produces open ports within a defined range"""
	def __init__(self, min_port, max_port):
		super(PortManager, self).__init__()
		self.max_port = max_port
		self.min_port = min_port
		self.low_port = min_port


	def get_port(self):
		self.low_port += 1
		return self.low_port

class StreamListing(object):
	"""docstring for StreamListing"""
	def __init__(self):
		super(StreamListing, self).__init__()
		self.streams = []

	def add(self, name):
		if not self.contains(name):
			self.append(name)

	def remove(self, name):
		if self.contains(name):
			self.remove(name)


	def contains(self, name):
		if self.streams.count(name) > 0 :
			return True
		else:
			return False

class Processor(object):
	"""Processor launches processes and configures data flow between processing and fusion components"""
	def __init__(self, zmq_context, push_port, fusion_port, return_port, process_count, process_path, fusion_path, identifier, data_filter, buffer_size=100, buffer_offset=50):
		super(Processor, self).__init__()

		#State
		self.running = False
		
		#Process settings
		self.process_count = int(process_count)
		self.identifier = identifier
		self.process_path = process_path
		self.fusion_path = fusion_path
		self.filter = data_filter
		
		#Controller Listing
		self.process_controlers = []
		self.fusion_controller = None

		#Buffer settings
		self.buffer_size = buffer_size
		self.buffer_array = []
		self.buffer_offset = buffer_offset
		self.buffer_count = 0
		
		#ZMQ context and sockets
		self.context = zmq_context
		self.push_socket = self.context.socket(zmq.PUSH)
		self.push_socket.bind("tcp://*:"+str(push_port))
		self.push_socket_port = str(push_port)
		self.fusion_socket = self.context.socket(zmq.PUSH)
		self.fusion_socket.connect("tcp://localhost:"+str(fusion_port))
		self.fusion_socket_port = str(fusion_port)

		#Port to send processed data to
		self.return_port = str(return_port)

	def start(self):
		"""Start processes"""
		for x, i in enumerate(range(self.process_count)):
			new_process = Process(identifier=self.identifier+"-p-"+str(i), path=self.process_path,port_in=self.push_socket_port,port_out=self.fusion_socket_port)
			# new_process = subprocess.Popen([self.process_path,self.push_socket_port,self.fusion_socket_port])
			self.process_controlers.append(new_process)

		# Fusion processes should accept one argument
		# self.fusion_controller = subprocess.Popen([self.fusion_path,self.fusion_socket_port, self.return_port, self.identifier])
		self.fusion_controller = Process(identifier=self.identifier, path=self.fusion_path, port_in=self.fusion_socket_port, port_out=self.return_port)

		self.running = True

		# Send synchronization to fusion
		self.fusion_socket.send(b'0')

	def stop(self):
		"""Stop processes and fusion"""
		for pc in self.process_controlers:
			pc.stop()
		self.fusion_controller.stop()
		
		#Reset controllers
		self._reset_controllers()

	def reload_processes(self, kill=False, rolling=False):
		"""Reloads all processes. By default will disable the processor and restart all processes. Enabling rolling will keep the processor running while reloading processes sequentially."""
		if rolling:
			for pc in self.process_controlers:
				pc.restart(kill=kill)
		else:
			self.running = False
			for pc in self.process_controlers:
				if kill:
					pc.kill()
				else:
					pc.stop()

			for pc in self.process_controlers:
				pc.start()
			self.running = True

	def reload_fusion(self, kill=False, hard_reload=True):
		"""Reloads the fusion process. Setting hard_reload to false will keep the processor running while the process is reloaded"""
		if hard_reload:
			self.running = False

		if kill:
			self.fusion_controller.kill()
		else:
			self.fusion_controller.stop()

		self.running = True



	def kill(self):
		"""Kill processes and fusion"""
		for pc in self.process_controlers:
			pc.kill()
		self.fusion_controller.kill()

		self._reset_controllers()

	def restart(self):
		self.stop()
		self.start()

	def _reset_controllers(self, fusion=True, process=True, running=True):
		if fusion:
			self.fusion_controller = None
		if process:
			self.process_controllers = []
		if running:
			self.running = False

	def list_pids(self):
		"""
		Returns a list of process pids	
		"""
		return [pc.get_pid() for pc in self.process_controlers]

	def process_count(self):
		"""
		Returns number of active processes

		*Ignores fusion controller
		"""

		return len(self.process_controlers)

		#This is 		

	def send(self, data):
		"""
		Incomplete function to push data to worker processes
		"""
		if self.running:
			self.push_socket.send_pyobj(data)

	def buffer(self, data):
		"""
		Add data to an internal buffer that sends data when the buffer is full and the offset is correct
		"""

		if self.running:
			data = data.split()
			if data[0] == self.filter:
				self.buffer_array.append((float(data[1]),float(data[2])))
				self.buffer_count += 1 

				if len(self.buffer_array) > self.buffer_size:
					self.buffer_array.pop(0)

				if self.buffer_count >= self.buffer_offset and len(self.buffer_array) == self.buffer_size:
					self.send(self.buffer_array)
					self.buffer_count = 0
			
class Process(object):
	"""Wrapper for subprocess that allows restarting of processes"""
	def __init__(self, start=True, path=None, port_in=None, port_out=None, identifier=None, args=[]):
		super(Process, self).__init__()
		self.path = path
		self.port_in = str(port_in)
		self.port_out = str(port_out)
		self.process_instance = None
		self.identifier=str(identifier)
		#Convert args to list
		self.args = args

		if start:
			self.start()

	def start(self):
		self.process_instance = subprocess.Popen([self.path,self.port_in,self.port_out,self.identifier] + self.args)

	def stop(self):
		self.process_instance.terminate()

	def kill(self):
		self.process_instance.kill()

	def get_pid(self):
		return self.process_instance.pid

	def restart(self, kill=False):
		#Stop process
		if kill:
			self.kill()
		else:
			self.stop()
		#Restart
		self.start()