# Data generator: allows to generate the input relations
import pandas as pd
from network import Network
from serializer import Serializer


class DataGenerator:
    def __init__(self, node_id, node_count, global_tuple_count) -> None:
        assert global_tuple_count >= node_count
        assert node_id < node_count

        self.node_id = node_id
        self.node_count = node_count
        self.global_tuple_count = global_tuple_count

    def _get_tuple_ids(self):
        tuples_per_node = self.global_tuple_count // self.node_count
        tuple_id_begin = self.node_id * tuples_per_node
        tuple_id_end = tuple_id_begin + tuples_per_node
        if self.node_id + 1 == self.node_count:
            tuple_id_end += self.global_tuple_count % self.node_count
        return range(tuple_id_begin, tuple_id_end)

    def create_relation_r(self):
        column_a = [(id + 1) for id in self._get_tuple_ids()]
        column_b = [id % 10 for id in self._get_tuple_ids()]
        return pd.DataFrame(data={"a": column_a, "b": column_b})

    def create_relation_s(self):
        column_a = [((id + 1) * 8128 % self.global_tuple_count) //
                    2 * 2 + 1 for id in self._get_tuple_ids()]
        column_c = [id % 10 for id in self._get_tuple_ids()]
        return pd.DataFrame(data={"a": column_a, "c": column_c})

    def split_df_into_partitions(self, relation, partition_key, num_partitions):
        # empty dict for partitions
        partitions = {}
        for i in range(num_partitions):
            partitions[i] = []  # init partitions as empty list

        # Converts DataFrame to list of dictionaries.
        # 'records' is predefined panda param
        rows = relation.to_dict('records')
        for row in rows:
            # set an partition id for each row
            partition_id = row[partition_key] % num_partitions
            # append rows to partitions
            partitions[partition_id].append(row)

        # Convert each lists to DataFrames
        for i in range(num_partitions):
            partitions[i] = pd.DataFrame(partitions[i])
        return partitions


# Sample code for using the data generator and doing the join locally
# python3 data_generator.py
if __name__ == "__main__":
    # Create the input relations `r` and `s`
    gen = DataGenerator(0, 2, 30)  # 2 partitions
    local_r = gen.create_relation_r()
    local_s = gen.create_relation_s()

    partitions_r = gen.split_df_into_partitions(local_r, 'a', 5)
    partitions_s = gen.split_df_into_partitions(local_s, 'a', 5)

    for i in range(5):
        print(f"Partition {i} of R:")
        print(partitions_r[i].head())
        print(f"Partition {i} of S:")
        print(partitions_s[i].head())

    # #  Setting up the Network
    # current_node_id = 1
    # all_ports = [5555, 5556]
    # other_ports = all_ports[:current_node_id] + all_ports[current_node_id+1:]
    # network = Network(all_ports[current_node_id], all_ports)

    # # Exchange partitions
    # received_r = network.send_and_receive_partitions(partitions_r, other_ports)
    # received_s = network.send_and_receive_partitions(partitions_s, other_ports)

    # # Perform join on received partitions
    # joined = pd.merge(received_r, received_s, on='a', how='inner')
    # print(sum(joined["b"] + joined["c"]))
