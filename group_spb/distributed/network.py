import zmq
import sys
import ast
from serializer import Serializer
import pandas as pd 
import data_generator


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


def shuffle_tables(ori_df, node_count):

    shuffled_tables_dict = {}

    for i in range(node_count):
        partition_table = ori_df[ori_df["a"] % node_count == i]
        shuffled_tables_dict[i] = partition_table

    return shuffled_tables_dict

# Sample code for using the network class, run in two sessions:
# node0: python3 network.py 0 "[5555, 5556]"
# node1: python3 network.py 1 "[5555, 5556]"
if __name__ == "__main__":
    # Get port as command line argument
    # ip_billy = "172.29.128.13"
    ip_billy = "localhost"
    assert len(sys.argv) == 3
    my_id = ast.literal_eval(sys.argv[1])
    all_ports = ast.literal_eval(sys.argv[2])
    other_ports = all_ports[:my_id] + all_ports[my_id+1:]
    network = Network(all_ports[my_id], all_ports, ip=ip_billy)
    node_count = len(all_ports)

    gen = data_generator.DataGenerator(my_id, node_count, 10)
    local_r = gen.create_relation_r()
    local_s = gen.create_relation_s()

    shuffled_r = shuffle_tables(local_r, node_count)
    shuffled_s = shuffle_tables(local_s, node_count)

    local_r = shuffled_r[my_id]
    local_s = shuffled_s[my_id]

    # Send a 'hello' data-frame to every other node
    for id, port in enumerate(all_ports):
        if id != my_id:
            df = pd.DataFrame(
                data={
                    "shuffled_r": [shuffled_r[id]],
                    "shuffled_s": [shuffled_s[id]]
                }
            )
            print(f"Sending to port number: {port}, id: {id}")
            network.client_send_message(port, Serializer.serialize_df(df))

    # Receive the 'hello'-df from all other nodes
    for port in other_ports:
        print(f"Receive from port number: {port}")
        message = network.server_receive_message()
        receive_df = Serializer.deserialize_df(message)
        print(f"{receive_df = }")
        local_r = pd.concat([local_r, receive_df['shuffled_r'][0]], axis=0)
        local_s = pd.concat([local_s, receive_df['shuffled_s'][0]], axis=0)

    joined = pd.merge(local_r, local_s, on='a', how='inner')

    # Eat all the oks
    for port in other_ports:
        network.client_expect_ok(port)

    if my_id != 0:
        network.client_send_message(all_ports[0], Serializer.serialize_df(joined))
        for port in other_ports:
            print(f"Receive result from port number: {port}")
            message = network.server_receive_message()
            df = Serializer.deserialize_df(message)
            print(f"{df = }")
    else:
        # send a dummy message so as to complete one "send receive cycle"
        df = pd.DataFrame(
            data={"message": ["Thanks"]}
        )

        for port in other_ports:
            print(f"Sending thanks to {port}")
            network.client_send_message(port, Serializer.serialize_df(df))

        for port in other_ports:
            print(f"Receive result from port number: {port}")
            message = network.server_receive_message()
            df = Serializer.deserialize_df(message)
            print(f"{joined = }")
            print(f"{df = }")
            joined = pd.concat([joined, df], axis=0)
        
        print(f"{joined = }")
        print("Total join sum: ", sum(joined["b"] + joined["c"]))

    # Eat all the oks
    for port in other_ports:
        # print("Waitin for OKs.")
        network.client_expect_ok(port)

    # Show how much data was sent
    print(f"Total data sent: {network.total_data_sent}")
