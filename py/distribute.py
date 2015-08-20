# Example distribute component for senmo
import zmq.green as zmq
import pipeline

context = zmq.Context()

raw_stream_in = context.socket(zmq.PULL)
raw_stream_in.bind("tcp://*:5000")

raw_stream_out = context.socket(zmq.PUB)
raw_stream_out.bind("tcp://*:5001")

processed_stream_in = context.socket(zmq.PULL)
processed_stream_in.bind("tcp://*:5003")

complete_stream_out = context.socket(zmq.PUB)
complete_stream_out.bind("tcp://*:5005")

control_socket = context.socket(zmq.REP)
control_socket.bind("tcp://*:6000")

poller = zmq.Poller()

poller.register(raw_stream_in, zmq.POLLIN)
poller.register(processed_stream_in, zmq.POLLIN)
poller.register(control_socket, zmq.POLLIN)

ports = pipeline.PortManager(7000,8000)

processors = {}


while True:
	socks = dict(poller.poll(1000))

	if raw_stream_in in socks and socks[raw_stream_in] == zmq.POLLIN:
		data = raw_stream_in.recv_string()
		# print("raw: "+data)
		#Unifiy the input stream
		raw_stream_out.send_string(data)

		# print(processors)

		for processor_name in processors:
			processors[processor_name].buffer(data)

	if processed_stream_in in socks and socks[processed_stream_in] == zmq.POLLIN:
		data = processed_stream_in.recv_string()
		print("processed: "+data)

	if control_socket in socks and socks[control_socket] == zmq.POLLIN:
		cmd = control_socket.recv_string().split()
		if cmd[0] == "ping":
			control_socket.send_string("pong")

		elif cmd[0] == "add":
			new_processor = pipeline.Processor(context, ports.get_port(), ports.get_port(), 5003, cmd[2], "../senmo-process/py-process-ecg.py","../senmo-fusion/py-fusion.py", cmd[1], cmd[3])
			processors[cmd[1]] = new_processor
			control_socket.send_string("Created processor")

		elif cmd[0] == "start":
			try:
				processors[cmd[1]].start()
				control_socket.send_string("Started processor: "+cmd[1])
			except KeyError:
				control_socket.send_string("Processor not found")

		elif cmd[0] == "stop":
			try:
				processors[cmd[1]].stop()
				control_socket.send_string("Stopped processor: "+cmd[1])
			except KeyError:
				control_socket.send_string("Processor not found")

		elif cmd[0] == "remove":
			try:
				processors.pop(cmd[1], None)
				control_socket.send_string("Removed processor: "+cmd[1])
			except KeyError:
				control_socket.send_string("Processor not found")


		elif cmd[0] == "list":
			control_socket.send_string(str(processors))

		else:
			control_socket.send_string("unknown command: "+str(cmd))

