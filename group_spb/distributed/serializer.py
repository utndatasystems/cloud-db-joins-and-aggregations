import io
import pandas as pd


class Serializer:
    @staticmethod
    def serialize_df(df):
        with io.BytesIO() as byte_array:
            df.to_pickle(byte_array)
            return byte_array.getvalue()

    @staticmethod
    def deserialize_df(bytes):
        with io.BytesIO(bytes) as byte_array:
            return pd.read_pickle(byte_array)
