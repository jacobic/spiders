import numpy as np
import pandas as pd
from functools import reduce

import astropy.units as u
from astropy import constants
from astropy.io.ascii import FloatType
from pyspark.ml import Transformer
from pyspark.ml.feature import SQLTransformer
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from typing import Union, List


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


class ColumnIndexer(Transformer):

    def _transform(self, df: DataFrame) -> DataFrame:
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


class GreatCircleDistanceDeg(Transformer):
    """
    An exact copy of astropy.coordinates.angle_utilities.angular_separation
    """

    def __init__(self, ra1Col, dec1Col, ra2Col, dec2Col, outputCol):
        self.ra1Col = ra1Col
        self.dec1Col = dec1Col
        self.ra2Col = ra2Col
        self.dec2Col = dec2Col
        self.outputCol = outputCol

    def _transform(self, df: DataFrame) -> DataFrame:
        a1, b1 = self.ra1Col, self.dec1Col
        a2, b2 = self.ra2Col, self.dec2Col
        df = df.withColumn('sda', f.expr(f'sin({a2} - {a1})'))
        df = df.withColumn('cda', f.expr(f'cos({a2} - {a1})'))
        df = df.withColumn('sb1', f.expr(f'sin({b1})'))
        df = df.withColumn('sb2', f.expr(f'sin({b2})'))
        df = df.withColumn('cb1', f.expr(f'cos({b1})'))
        df = df.withColumn('cb2', f.expr(f'cos({b2})'))
        df = df.withColumn('n1', f.expr('cb2 * sda'))
        df = df.withColumn('n2', f.expr('cb1 * sb2 - sb1 * cb2 * cda'))
        df = df.withColumn('n3', f.expr('sb1 * sb2 + cb1 * cb2 * cda'))
        df = df.withColumn('n4', f.expr('sqrt(pow(n1, 2) + pow(n2, 2))'))
        df = df.withColumn(self.outputCol, f.expr('atan2(n4, n3)'))
        tmp = ['sda', 'cda', 'sb1', 'sb2', 'cb1', 'cb2', 'n1', 'n2', 'n3', 'n4']
        df = df.drop(*tmp)
        return df


class GalacticEquatorial(SQLTransformer):

    def __init__(self, lCol, bCol, raCol, decCol):
        self.lCol = lCol
        self.bCol = bCol
        self.raCol = raCol
        self.decCol = decCol

        alpha_g = 192.85948
        delta_g = 27.12825
        l_ncp = 122.93192

        statement = f""" SELECT *, 
        acos( (sin({self.bCol}) * cos({delta_g}) 
                - cos({self.bCol}) * sin({delta_g}) * sin({self.lCol} 
                - {l_ncp})) /cos({self.decCol})) 
        + { alpha_g} 
        
        AS {self.raCol}
        
       FROM (SELECT *, asin(
       
        (sin({self.bCol}) * sin({delta_g})) + cos({self.bCol})*cos({
        delta_g})*sin({self.lCol} - {l_ncp})
       
       ) AS {self.decCol}
       
       FROM __THIS__
       )
       """
        super().__init__(statement=statement)


class ProperVelocity(Transformer):

    def __init__(self, zgalCol, zclusCol, outputCol, outUnit='km/s'):
        self.zgalCol = zgalCol
        self.zclusCol = zclusCol
        self.outputCol = outputCol
        self.outUnit = outUnit

    def _transform(self, df) -> DataFrame:
        z, z_clus = self.zgalCol, self.zclusCol
        c = constants.c.to(u.Unit(self.outUnit)).value
        expression = f'{c} * ({z} - {z_clus}) / (1 + {z_clus})'
        df = df.withColumn(self.outputCol, f.expr(expression))
        return df
