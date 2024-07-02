import zmq
import sys
import ast
import time
from serializer import Serializer
from data_generator_l import DataGenerator
import pandas as pd


class Network:
    def __init__(self, my_port, all_ports):
        self.total_data_sent = 0
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{my_port}")
        print(f"Server started: {my_port}")

        self.nodes = {}
        for port in all_ports:
            if port == my_port:
                continue
            req_socket = self.context.socket(zmq.REQ)
            req_socket.connect(f"tcp://localhost:{port}")
            self.nodes[port] = req_socket
            print(f"Connected to: {port}")

    def server_receive_message(self):
        print("Waiting to receive message")
        try:
            message = self.socket.recv()
            self.socket.send(b"ok")  # Send acknowledgment back
            print("Message received and acknowledgment sent")
            return message
        except Exception as e:
            print(f"Error receiving message: {e}")
            return None

    def client_send_message(self, port, message):
        print(f"Sending message to port {port}")
        try:
            self.total_data_sent += len(message)
            self.nodes[port].send(message)
            self.nodes[port].recv()  # Wait for acknowledgment
            print(f"Message sent to port {port}")
        except Exception as e:
            print(f"Error sending message to port {port}: {e}")


def node_operation(my_id, all_ports):
    print(f"Node {my_id} is starting")
    network = Network(all_ports[my_id], all_ports)
    node_count = len(all_ports)
    other_ports = all_ports[:my_id] + all_ports[my_id + 1:]

    # Create input relations
    gen = DataGenerator(my_id, node_count, 1000000)
    local_r = gen.create_relation_r()
    local_s = gen.create_relation_s()

    time.sleep(2)  # Short delay: ensure all sockets initialized

    if my_id == 0:
        # Node 0: Collect data and perform the join
        data_r = [local_r]
        data_s = [local_s]

        # Receive data from other nodes
        for _ in other_ports:
            print(f"Node 0 waiting to receive")
            r_message = network.server_receive_message()
            s_message = network.server_receive_message()
            if r_message:
                try:
                    data_r.append(Serializer.deserialize_df(r_message))
                    print("Node 0 received and deserialized R")
                except Exception as e:
                    print(f"Error deserializing r_message: {e}")
            if s_message:
                try:
                    data_s.append(Serializer.deserialize_df(s_message))
                    print("Node 0 received and deserialized S")
                except Exception as e:
                    print(f"Error deserializing s_message: {e}")

        # join
        full_r = pd.concat(data_r)
        full_s = pd.concat(data_s)
        print(full_r.columns)
        joined = pd.merge(full_r, full_s, on='a', how='inner')
        print(joined.columns)
        print(sum(joined["b"] + joined["c"]))
    else:
        # Other nodes: Send data to node 0
        print(f"Node {my_id} is preparing to send data to node 0")
        network.client_send_message(all_ports[0], Serializer.serialize_df(local_r))
        print(f"Node {my_id} sent relation R to node 0")
        network.client_send_message(all_ports[0], Serializer.serialize_df(local_s))
        print(f"Node {my_id} sent relation S to node 0")

    print(f"Node {my_id} total data sent: {network.total_data_sent}")


if __name__ == "__main__":
    assert len(sys.argv) == 3
    my_id = ast.literal_eval(sys.argv[1])
    all_ports = ast.literal_eval(sys.argv[2])
    node_operation(my_id, all_ports)
