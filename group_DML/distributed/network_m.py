import zmq
import sys
import ast
from serializer import Serializer
import pandas as pd
from data_generator import DataGenerator
import time


class Network:
    def __init__(self, my_port, all_ports):
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
            socket.connect(f"tcp://localhost:{port}")
            self.nodes[port] = socket
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

    def client_expect_ok(self, port):
        # print(f"client_expect_ok {port}")
        self.nodes[port].recv()

    def send_and_receive_partitions(self, partitions, other_ports):
        num_nodes = len(other_ports) + 1  # + current node

        for partition_id in partitions:
            # for debug jsut in case
            if partition_id >= num_nodes:
                raise IndexError("Partition ID is out of range of available nodes")

            if partition_id < len(other_ports):
                # Get the partition data
                partition = partitions[partition_id]
                # Find the target port for this partition
                target_port = other_ports[partition_id]
                # Serialize the partition DataFrame to bytes
                serialized_partition = Serializer.serialize_df(partition)
                # Send the serialized partition to the target node
                self.client_send_message(target_port, serialized_partition)

        # Empty list for the received partitions
        received_partitions = []
        # Iterate through other nodes to get partitions
        for port in other_ports:
            # Receive message from a node
            message = self.server_receive_message()
            # Deserialize from bytes back to DataFrame
            received_partition = Serializer.deserialize_df(message)
            # Append to list
            received_partitions.append(received_partition)

        # Concat all received partitions into one DataFrame and return it
        all_partitions = pd.concat(received_partitions, ignore_index=True)
        return all_partitions

    def receive_process(self, other_ports, local_r, local_s, my_node):
        # Node 0: Collect data and perform the join
        data_r = [local_r]
        data_s = [local_s]

        # Receive data from other nodes
        for _ in other_ports:
            print(f"Node {my_node} waiting to receive")
            r_message = self.server_receive_message()
            s_message = self.server_receive_message()
            if r_message:
                try:
                    data_r.append(Serializer.deserialize_df(r_message))
                    print(f"Node {my_node} received and deserialized R")
                except Exception as e:
                    print(f"Error deserializing r_message: {e}")
            if s_message:
                try:
                    data_s.append(Serializer.deserialize_df(s_message))
                    print(f"Node {my_node} received and deserialized S")
                except Exception as e:
                    print(f"Error deserializing s_message: {e}")

        # join
        full_r = pd.concat(data_r)
        full_s = pd.concat(data_s)
        print(full_r.columns)
        joined = pd.merge(full_r, full_s, on='a', how='inner')
        print(joined.columns)
        print(sum(joined["b"] + joined["c"]))
        return joined

    def send_partition(self, local_r, local_s, port, node):
        print(f"Node {my_id} is preparing to send data to node {node}")
        self.client_send_message(port,
                                 Serializer.serialize_df(local_r))
        print(f"Node {my_id} sent relation R to node {node}")
        self.client_send_message(port,
                                 Serializer.serialize_df(local_s))
        print(f"Node {my_id} sent relation S to node {node}")


def node_operation(my_id, all_ports):
    print(f"Node {my_id} is starting")
    network = Network(all_ports[my_id], all_ports)
    node_count = len(all_ports)
    other_ports = all_ports[:my_id] + all_ports[my_id + 1:]

    # Create input relations
    gen = DataGenerator(my_id, node_count, 1000000)
    local_r = gen.create_relation_r()
    local_s = gen.create_relation_s()

    partitions_r = gen.split_df_into_partitions(local_r, 'a', node_count)
    partitions_s = gen.split_df_into_partitions(local_s, 'a', node_count)
    print("will start in 15 seconds")
    time.sleep(15)  # Short delay: ensure all sockets initialized
    print(f"Node {my_id} started operating")

    for partition, port in enumerate(all_ports):
        if my_id == partition:
            joind_partition = network.receive_process(
                other_ports,
                partitions_r[partition],
                partitions_s[partition], my_id)
        else:
            network.send_partition(partitions_r[partition],
                                   partitions_s[partition],
                                   port, partition)
    print(len(joind_partition))
    print(f"Node {my_id} total data sent: {network.total_data_sent}")
    

# Sample code for using the network class, run in two sessions:
# node0: python3 network.py 0 "[5555, 5556]"
# node1: python3 network.py 1 "[5555, 5556]"
if __name__ == "__main__":
    # Get port as command line argument
    assert len(sys.argv) == 3
    my_id = ast.literal_eval(sys.argv[1])
    all_ports = ast.literal_eval(sys.argv[2])
    node_operation(my_id, all_ports)
