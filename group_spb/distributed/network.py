import zmq
import sys
import ast
from serializer import Serializer
import pandas as pd


class Network:
    def __init__(self, my_port, all_ports, ip="localhost"):
        # Start own server
        self.total_data_sent = 0
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{my_port}")
        print(f"Server started: {my_port}")

        # Connect to other nodes
        self.nodes = {}
        for port in all_ports:
            if port == my_port:
                continue
            socket = self.context.socket(zmq.REQ)
            socket.connect(f"tcp://{ip}:{port}")
            self.nodes[port] = socket
            print(f"Connected to: {port}")

    def server_receive_message(self):
        # print(f"server_receive_message")
        message = self.socket.recv()
        self.socket.send(b"ok")
        return message

    def client_send_message(self, port, message):
        # print(f"client_send_message {port}")
        self.total_data_sent += len(message)
        self.nodes[port].send(message)

    def client_expect_ok(self, port):
        # print(f"client_expect_ok {port}")
        self.nodes[port].recv()


# Sample code for using the network class, run in two sessions:
# node0: python3 network.py 0 "[5555, 5556]"
# node1: python3 network.py 1 "[5555, 5556]"
if __name__ == "__main__":
    # Get port as command line argument
    ip_billy = "172.29.222.223"
    assert len(sys.argv) == 3
    my_id = ast.literal_eval(sys.argv[1])
    all_ports = ast.literal_eval(sys.argv[2])
    other_ports = all_ports[:my_id] + all_ports[my_id+1:]
    network = Network(all_ports[my_id], all_ports, ip="")
    node_count = len(all_ports)

    # Create a fake-relation-data-frame
    df = pd.DataFrame(
        data={"message": ["hello", "from", f"{all_ports[my_id]}"]})

    # Send a 'hello' data-frame to every other node
    for port in other_ports:
        print(f"Sending to {port}")
        network.client_send_message(port, Serializer.serialize_df(df))

    # Receive the 'hello'-df from all other nodes
    for _ in other_ports:
        message = network.server_receive_message()
        df = Serializer.deserialize_df(message)
        print(df)

    # Eat all the oks
    for port in other_ports:
        network.client_expect_ok(port)

    # Show how much data was sent
    print(f"Total data sent: {network.total_data_sent}")
