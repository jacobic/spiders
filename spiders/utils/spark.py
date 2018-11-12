from functools import reduce
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from typing import Union, List

import pandas as pd
import numpy as np


def buckets(x: object, attr: str, bins: np.ndarray) -> str:
    label = pd.cut([getattr(x, attr)], bins=bins).astype(str)[0]
    return f'{attr}: {label}'


def append(s1: pd.Series, s2: Union[pd.Series, List[pd.Series]]) -> pd.Series:
    return s1.append(s2)


def cat(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([df1, df2])


def unionAll(dfs: List[DataFrame]) -> DataFrame:
    return reduce(DataFrame.union, dfs)


def typed_udf(return_type):
    """Make a UDF decorator with the given return type.

    Example usage:

    >>> @typed_udf(t.IntegerType())
    ... def increment(x):
    ...     return x + 1
    ...
    >>> df = df.withColumn('col_plus_1', increment('col'))

    See http://johnpaton.net/posts/clean-spark-udfs for more detail.

    Args:
        return_type (pyspark.sql.types type): the type that will be
            output by the function being decorated

    Returns:
        function: Typed UDF decorator

    """
    def _typed_udf_wrapper(func):
        return f.udf(func, return_type)

    return _typed_udf_wrapper


def withColumnIndex(df: DataFrame):
    """
    Add index to spark DataFrame with column labeled "idx".
    """
    # Create new column names
    cols = df.schema.names
    _cols = cols + ['idx']

    # Add Column index
    def add_idx(row, idx):
        return row + (idx,)

    # Zip and rename columns.
    _df = df.rdd.zipWithIndex().map(lambda x: add_idx(*x)).toDF()
    columns = dict(zip(_cols, _df.columns))
    return _df.select([f.col(v).alias(k) for k, v in columns.items()])
