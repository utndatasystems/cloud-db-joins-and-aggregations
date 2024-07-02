import zmq
import sys
import ast
from serializer import Serializer
import pandas as pd 
import data_generator
import time


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
    # configurations
    ip_billy = "localhost"
    num_global_tuples = 1_000_000

    assert len(sys.argv) == 4
    my_id = ast.literal_eval(sys.argv[1])
    all_ports = ast.literal_eval(sys.argv[2])
    shuffle = sys.argv[3] in ["true", "True"]
    other_ports = all_ports[:my_id] + all_ports[my_id+1:]
    network = Network(all_ports[my_id], all_ports, ip=ip_billy)
    node_count = len(all_ports)

    gen = data_generator.DataGenerator(my_id, node_count, num_global_tuples)
    local_r = gen.create_relation_r()
    local_s = gen.create_small_relation_s(table_size=20)

    if shuffle:
        local_s = gen.create_relation_s()

        shuffled_r = shuffle_tables(local_r, node_count)
        shuffled_s = shuffle_tables(local_s, node_count)

        local_r = shuffled_r[my_id]
        local_s = shuffled_s[my_id]

    if shuffle:
        # Send a 'hello' data-frame to every other node
        for id, port in enumerate(all_ports):
            if id != my_id:
                df = pd.DataFrame(
                    data={
                        "From": port,
                        "shuffled_r": [shuffled_r[id]],
                        "shuffled_s": [shuffled_s[id]]
                    }
                )
                print(f"Sending to port number: {port}, id: {id}")
                print("shuffled_r\n")
                print(df['shuffled_r'])
                print()
                print("shuffled_s\n")
                print(df['shuffled_s'])
                print()
                network.client_send_message(port, Serializer.serialize_df(df))


    # Receive the 'hello'-df from all other nodes
    if shuffle:
        for _ in other_ports:
            message = network.server_receive_message()
            receive_df = Serializer.deserialize_df(message)
            print(f"Receive from port number: {receive_df['From'][0]}")
            print("shuffled_r\n")
            print(receive_df['shuffled_r'])
            print()
            print("shuffled_s\n")
            print(receive_df['shuffled_s'])
            print()
            local_r = pd.concat([local_r, receive_df['shuffled_r'][0]], axis=0)
            local_s = pd.concat([local_s, receive_df['shuffled_s'][0]], axis=0)

    # local inner join by every node
    joined = pd.merge(local_r, local_s, on='a', how='inner')

    if shuffle:
        # Eat all the oks
        for port in other_ports:
            network.client_expect_ok(port)

    # this is to add a delay for synchronisation
    time.sleep(1)

    # all non-primary nodes send out data to the primary
    if my_id != 0:
        
        joined_data = pd.DataFrame(
            data={
                "From": [all_ports[my_id]],
                "data": [joined]
            }
        )

        network.client_send_message(all_ports[0], Serializer.serialize_df(joined_data))

    else:

        for port in other_ports:
            
            message = network.server_receive_message()
            # message_ = network.server_receive_message()
            df = Serializer.deserialize_df(message)
            print(f"Receive result from port number: {df['From'][0]}")
            print(f"{joined = }")
            print(f"{df['data'][0] = }")
            joined = pd.concat([joined, df["data"][0]], axis=0)
        
        print(f"{joined = }")
        print("Total join sum: ", sum(joined["b"] + joined["c"]))

    # Eat the oks from the primary node
    if my_id != 0:
        network.client_expect_ok(all_ports[0])

    # Show how much data was sent
    print(f"Total data sent: {network.total_data_sent}")
