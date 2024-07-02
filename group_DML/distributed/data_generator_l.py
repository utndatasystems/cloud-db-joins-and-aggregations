# Data generator: allows to generate the input relations
import pandas as pd


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


# Sample code for using the data generator and doing the join locally
# python3 data_generator.py
if __name__ == "__main__":
    # Create the input relations `r` and `s` 1000000
    gen = DataGenerator(0, 1, 5)
    local_r = gen.create_relation_r()
    local_s = gen.create_relation_s()

    # Print for debugging :)
    print("Relation R")
    print(local_r.head())
    print("Relation S")
    print(local_s.head())
    print("----------")

    # Create the full relation `s` and join
    joined = pd.merge(local_r, local_s, on='a', how='inner')
    print(sum(joined["b"] + joined["c"]))
    
    print(joined["b"])
