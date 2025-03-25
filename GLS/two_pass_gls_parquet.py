import os, sys
import numpy as np

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "das_decennial"))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "dependencies"))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as T

from GLS.das_decennial.programs.nodes.node4sparksql import sparkTypeDict2StructType
from GLS.dependencies.das_utils.das_utils import repartitionNodesEvenly


def from_symmetric_mat_to_bytes(row_dict, key):
    if row_dict[key] is None:
        return None
    return row_dict[key][np.triu_indices(row_dict[key].shape[0])].tobytes()


def from_bytes_to_square_mat(input_bytes, n):
    if input_bytes is None:
        return None
    if np.sqrt(np.array(input_bytes).size) < n ** 2:
        return from_bytes_to_symmetric_mat(input_bytes)
    return from_bytes_to_unsymmetric_mat(input_bytes)


def from_bytes_to_symmetric_mat(input_bytes):
    if input_bytes is None:
        return None
    data = np.array(input_bytes)
    # Since \sum_{i=1}^n i = n (n + 1) / 2, after solving for n in, \sum_{i=1}^n i == k, we have n = (sqrt(1 + 8 * k) - 1) / 2:
    n_float = (np.sqrt(1 + 8 * data.size) - 1) / 2
    n = int(np.round(n_float))
    assert np.abs(n_float - n) < 1e-14
    res = np.zeros((n, n))
    res[np.triu_indices(n)] = data
    if np.any(np.isinf(np.diag(res))):
        assert np.all(np.isinf(np.diag(res)))
        return np.diag(np.full(n, np.inf))
    return res + res.T - np.diag(np.diag(res))


def from_bytes_to_unsymmetric_mat(input_bytes):
    if input_bytes is None:
        return None
    data = np.array(input_bytes)
    n = int(np.round(np.sqrt(data.size)))
    return data.reshape((n, n))


class GLSTwoPassNode:
    """ Should only have np.ndarrays and a geocode string. Arrays can be null"""
    spark_type_dict = {
        # (Type, nullable) pairs
        'covar_cond_g'         : (T.BinaryType(), True),
        'beta_hat_cond_g'      : (T.BinaryType(), True),
        'beta_hat_cond_g_minus': (T.BinaryType(), True),
        'a_mat'                : (T.BinaryType(), True),
        'covar_tilde'          : (T.BinaryType(), True),
        'covar_cond_g_minus'   : (T.BinaryType(), True),
        'beta_tilde'           : (T.BinaryType(), True),
        'geocode'              : (T.StringType(), False),
        'num_siblings'         : (T.IntegerType(), True)
    }
    spark_type = sparkTypeDict2StructType(spark_type_dict)

    def __init__(self, row, covar_cond_g_minus_is_symmetric=True):
        # See subsection of paper entitled "Computational Considerations" for more detail on why we allow for covar_cond_g_minus to be
        # saved as a nonsymmetric array (ie, usng 'covar_cond_g_minus_is_symmetric=False'). In short, in some cases this can increase
        # the computational efficiency of est_cis.py, which we plan on supporting more fully in a future code update.

        assert isinstance(row[1], dict)
        row = row[1]
        if covar_cond_g_minus_is_symmetric:
            covar_cond_g_minus = from_symmetric_mat_to_bytes(row, 'covar_cond_g_minus')
        else:
            covar_cond_g_minus = row['covar_cond_g_minus'].tobytes() if row['covar_cond_g_minus'] is not None else None

        self.struct = {
            'covar_cond_g'         : from_symmetric_mat_to_bytes(row, 'covar_cond_g'),
            'beta_hat_cond_g'      : row['beta_hat_cond_g'].tobytes() if row['beta_hat_cond_g'] is not None else None,
            'beta_hat_cond_g_minus': row['beta_hat_cond_g_minus'].tobytes() if row['beta_hat_cond_g_minus'] is not None else None,
            'a_mat'                : row['a_mat'].tobytes() if row['a_mat'] is not None else None,
            'covar_tilde'          : from_symmetric_mat_to_bytes(row, 'covar_tilde'),
            'covar_cond_g_minus'   : covar_cond_g_minus,
            'beta_tilde'           : row['beta_tilde'].tobytes() if row['beta_tilde'] is not None else None,
            'geocode'              : row['geocode'],
            'num_siblings'         : row['num_siblings']
        }


class GLSEntityEst:
    """ Should only have np.ndarrays and a geocode string. Arrays can be null"""
    spark_type_dict = {
        # (Type, nullable) pairs
        'covar_mat': (T.BinaryType(), True),
        'beta_hat' : (T.BinaryType(), True),
        'geocode'  : (T.StringType(), False),
    }

    spark_type = sparkTypeDict2StructType(spark_type_dict)
    def __init__(self, row):
        # row input format is: (geographic_entity, entity_estimate, entity_estimate_covar)
        self.struct = {
            'covar_mat': from_symmetric_mat_to_bytes({"covar_mat": row[2]}, 'covar_mat'),
            'beta_hat' : row[1].tobytes() if row[1] is not None else None,
            'geocode'  : row[0],
        }


class GLSTwoPassSummedNode:
    """ Should only have np.ndarrays and a geocode string. Arrays can be null"""
    spark_type_dict = {
        # (Type, nullable) pairs
        'geocode'                   : (T.StringType(), False),
        "sum_covar_cond_g_minus_inv": (T.BinaryType(), True),
        "sum_beta_hat_cond_g_minus" : (T.BinaryType(), True)
    }

    spark_type = sparkTypeDict2StructType(spark_type_dict)
    def __init__(self, row):
        row = row[1]
        self.struct = {
            "sum_covar_cond_g_minus_inv": from_symmetric_mat_to_bytes(row, 'sum_covar_cond_g_minus_inv'),
            "sum_beta_hat_cond_g_minus" : None if row["sum_beta_hat_cond_g_minus"] is None else row["sum_beta_hat_cond_g_minus"].tobytes(),
            'geocode'                   : row['geocode'],
        }


def write_parquet(rdd, save_path, spark_type_class, save_as_csv=False):
    spark = SparkSession.builder.getOrCreate()
    print(f"\t\tWriting to {save_path}")
    rdd = rdd.map(lambda row: (spark_type_class(row).struct,))
    df = spark.createDataFrame(rdd, T.StructType([T.StructField(spark_type_class.__name__, spark_type_class.spark_type)])).select(spark_type_class.__name__ + ".*")

    if spark_type_class.__name__ == "GLSTwoPassNode":
        df = df.selectExpr([f"bin2da({array_name}) as {array_name}_da" for array_name in ("covar_cond_g", "beta_hat_cond_g", "beta_hat_cond_g_minus", "a_mat", "covar_tilde", "covar_cond_g_minus", "beta_tilde")] + ["geocode", "num_siblings"])
    elif spark_type_class.__name__ == "GLSTwoPassSummedNode":
        df = df.selectExpr([f"bin2da({array_name}) as {array_name}_da" for array_name in ("sum_covar_cond_g_minus_inv", "sum_beta_hat_cond_g_minus")] + ["geocode"])
    elif spark_type_class.__name__ == "GLSEntityEst":
        df = df.selectExpr([f"bin2da({array_name}) as {array_name}_da" for array_name in ("covar_mat", "beta_hat")] + ["geocode"])
    else:
        assert spark_type_class.__name__ == "EntityAndQueryCI"

    df.write.mode("overwrite").parquet(save_path, compression="zstd")
    print(f"\t\tWriting to {save_path} done")
    if save_as_csv:
        print(f"\t\tWriting to {save_path + '.csv'}")
        df.coalesce(100).write.mode("overwrite").csv(save_path + ".csv", header=True)
        print(f"\t\tWriting to {save_path + '.csv'} done")


def read_parquet_gls_two_pass_node(load_path, df=None, join_col=None, partitions=None):
    spark = SparkSession.builder.getOrCreate()
    df_gls_tpn = spark.read.parquet(*load_path) if isinstance(load_path, list) else spark.read.parquet(load_path)
    if df is not None:
        assert join_col is not None
        df_gls_tpn = df_gls_tpn.join(df, join_col)
        if partitions is not None:
            df_gls_tpn = df_gls_tpn.repartition(partitions)

    def glsNodeFromSpark(row):
        # The following ensures res has the fields 'geocode' and 'num_siblings', and, when df input of read_parquet_gls_two_pass_node() is not None, also any remaining fields in the df input:
        res = {k: v for k, v in row.asDict().items() if k[-3:] != "_da"}
        res['a_mat'] = from_bytes_to_unsymmetric_mat(row['a_mat_da'])

        for est_name in ['beta_hat_cond_g', 'beta_hat_cond_g_minus', 'beta_tilde']:
            res[est_name] = None if row[est_name + "_da"] is None else np.array(row[est_name + "_da"])

        for array_name in ['covar_cond_g', 'covar_tilde', 'covar_cond_g_minus']:
            res[array_name] = from_bytes_to_symmetric_mat(row[array_name + "_da"])

        # Since we plan on supporting allowing covar_cond_g_minus to be encoded as either a symmetric or a nonsymmetric matrix when writting the two-pass-node dataframes in the future, allow
        # for it to be read using either encoding:
        # res['covar_cond_g_minus'] = from_bytes_to_square_mat(row['covar_cond_g_minus_da'], n_rows)

        return res['geocode'], res
    return df_gls_tpn.rdd.map(lambda row: glsNodeFromSpark(row))


def read_parquet_gls_entity_est(load_path, df=None, join_col=None, partitions=None):
    spark = SparkSession.builder.getOrCreate()
    df_gls_tpn = spark.read.parquet(*load_path) if isinstance(load_path, list) else spark.read.parquet(load_path)
    if df is not None:
        assert join_col is not None
        df_gls_tpn = df_gls_tpn.join(df, join_col)
        if partitions is not None:
            df_gls_tpn = df_gls_tpn.repartition(partitions)

    def glsEntityEstFromSpark(row):
        # The following ensures res has the fields 'geocode' and 'num_siblings', and, when df input of read_parquet_gls_two_pass_node() is not None, also any remaining fields in the df input:
        res = {k: v for k, v in row.asDict().items() if k[-3:] != "_da"}

        res['covar_mat'] = from_bytes_to_symmetric_mat(row['covar_mat_da'])
        res['beta_hat'] = None if row["beta_hat_da"] is None else np.array(row["beta_hat_da"])
        # The functions in est_cis.py expect row[1] to be a tuple containing the values of res in the following order:
        return res['geocode'], tuple(res[k] for k in ["geocode", "beta_hat", "covar_mat"])
    return df_gls_tpn.rdd.map(lambda row: glsEntityEstFromSpark(row))


def checkpoint(rdd, levels_dict, checkpoint_dir, spark_type_class=GLSTwoPassNode, read_function=read_parquet_gls_two_pass_node, repartition=True, partition_size=None):
    if repartition:
        assert partition_size is not None
        rdd = repartitionNodesEvenly(rdd, 0, levels_dict, is_key_value_pairs=True, partition_size=partition_size).map(lambda row: (row['geocode'], row))
    write_parquet(rdd, checkpoint_dir, spark_type_class)
    return read_function(checkpoint_dir)


def checkpoint_df(rdd, checkpoint_dir, spark_type_class=GLSTwoPassNode):
    spark = SparkSession.builder.getOrCreate()
    write_parquet(rdd, checkpoint_dir, spark_type_class)
    return spark.read.parquet(checkpoint_dir)


class EntityAndQueryCI:
    """ Should only have np.ndarrays and a geocode string. Arrays can be null"""
    spark_type_dict = {
        # (Type, nullable) pairs
        'geocode' : (T.StringType(), False),
        'query'   : (T.StringType(), False),
        'alpha'   : (T.DoubleType(), False),
        'estimate': (T.DoubleType(), False),
        'std_dev' : (T.DoubleType(), False),
        'moe'     : (T.DoubleType(), False),
    }

    spark_type = sparkTypeDict2StructType(spark_type_dict)
    def __init__(self, row):
        # Everything else but the 3 SumOverGroupedQuery constructor arguments (array_dims, add_over_margins and groupings)
        # are calculable from these (and is, in the constructor). Adding 'shape' and 'name' for convenience
        self.struct = {
            'geocode' : row[0],
            'query'   : row[1],
            'alpha'   : row[2],
            'estimate': row[3],
            'std_dev' : row[4],
            'moe'     : row[5],
        }


def read_parquet_entity_and_query_ci(load_path):
    spark = SparkSession.builder.getOrCreate()
    rdd = spark.read.parquet(load_path).rdd

    def EntityAndQueryCIFromSpark(row):
        col_names = ["geocode", "query", "alpha", "estimate", "std_dev", "moe"]
        res = {col: row[col] for col in col_names}
        return res["geocode"], res
    return rdd.map(lambda row: EntityAndQueryCIFromSpark(row))
