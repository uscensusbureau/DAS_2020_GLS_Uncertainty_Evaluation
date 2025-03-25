"""
This file contains class to convert python objects to Spark types / structures, which can be natively stored in a Spark DataFrame.
(The primary goal is to work with NMF, but the rest of the GeounitNode structure is also implemented in case we want to switch the DAS
to use this format for saving, loading, joins and repartitionings, since it is ~10x more lightweight than pickled (even zipped) python objects)
In each class ".struct" attrubute contains the structure that can be ingested as native by a column in Spark DataGrame, and .spark_type is the
schema / data structure for Spark to be able to interpret it.

(Not sure the following is actually true, but not deleting it, just to keep that in mind, if it is the case)
IMPORTANT: THE ORDER IN .struct AND IN spark_type SHOULD COINCIDE! (Python dicts are guarateed to be ordered as created since version 3.7)
"""

import pyspark.sql.types as T
from GLS.das_decennial.das_constants import CC


def sparkTypeDict2StructType(d):
    return T.StructType([T.StructField(name, *t) for name, t in d.items()])


class SumOverGroupedQuery2Spark:
    """ Should only have dicts and lists, possibly nested"""
    spark_type_dict = {
        # (Type, nullable) pairs
        'array_dims'      : (T.ArrayType(T.IntegerType()), False),
        'groupings'       : (T.MapType(T.ShortType(), T.ArrayType(T.ArrayType(T.IntegerType())), False), True),
        'add_over_margins': (T.ArrayType(T.ShortType()), True),
        'shape'           : (T.ArrayType(T.ShortType()), True),
        'name'            : (T.StringType(), False),
    }

    spark_type = sparkTypeDict2StructType(spark_type_dict)

    ## Inits are not needed the spark conversion works fine by type, only picking the attributes indicated in the schema,
    ## unless you need non-trivial conversion

    def __init__(self, query):
        # Everything else but the 3 SumOverGroupedQuery constructor arguments (array_dims, add_over_margins and groupings)
        # are calculable from these (and is, in the constructor). Adding 'shape' and 'name' for convenience
        self.struct = {
            'array_dims'       : query.array_dims,
            'groupings'        : {k:[list(g) for g in v] for k,v in query.groupings.items()} if query.groupings is not None else None,
            'add_over_margins' : query.add_over_margins,
            'shape'            : query.queryShape(),
            'name'             : query.name,
        }


class DPQueryConvertibleToSparkType:
    """ Should only have dicts and lists, possibly nested"""
    spark_type_dict = {
        # (Type, nullable) pairs
        "query"                 : (SumOverGroupedQuery2Spark.spark_type, False),
        "DPanswer"              : (T.BinaryType(), True),
        'plb'                   : (T.DoubleType(), False),
        'plb_num'               : (T.LongType(), True),
        'plb_denom'             : (T.LongType(), True),
        'DPMechanism'           : (T.StringType(), False),
        'Var'                   : (T.DoubleType(), False),
        'name'                  : (T.StringType(), False),
        'mechanism_sensitivity' : (T.ShortType(), False),
        'budget_id'             : (T.StringType(),False),
    }

    spark_type = sparkTypeDict2StructType(spark_type_dict)

    def __init__(self, dp_query):
        self.struct = {
            'query':                 SumOverGroupedQuery2Spark(dp_query.query).struct,
            'DPanswer':              dp_query.DPanswer.tobytes() if dp_query.plb > 0 else None,
            'plb':                   float(dp_query.plb),
            'DPMechanism':           dp_query.DPmechanism,
            'Var':                   float(dp_query.Var),
            'name':                  dp_query.name,
            'mechanism_sensitivity': int(dp_query.mechanism_sensitivity),
            'budget_id':             dp_query.budget_id
        }
        plb_fields_values = (dp_query.plb.numerator, dp_query.plb.denominator) if hasattr(dp_query.plb, "numerator") else (None, None)
        self.struct.update(dict(zip(("plb_num", "plb_denom"), plb_fields_values)))


class ConstraintConvertibleToSparkType:
    """ Should only have dicts and lists, possibly nested"""
    multi_query_spark_type_dict = {
        "queries": (T.ArrayType(SumOverGroupedQuery2Spark.spark_type), True),
        "coeff":   (T.ArrayType(T.IntegerType()), True),
        "shapes":  (T.ArrayType(T.ArrayType(T.IntegerType())), True),
    }

    spark_type_dict = {
        "name":  (T.StringType(), False),
        "rhs":   (T.ArrayType(T.LongType()), False),   # Upper bounds on people in GQ can exceed Integer
        "sign":  (T.StringType(), False),
        "query": (sparkTypeDict2StructType(multi_query_spark_type_dict), False),
    }

    spark_type = sparkTypeDict2StructType(spark_type_dict)

    def __init__(self, c):
        self.struct = {
            "name":   c.name,
            "rhs":   c.rhs.tolist(),
            "sign":  c.sign,
            "query": {"queries": [SumOverGroupedQuery2Spark(q).struct if cf!=0 else None for q, cf in zip(c.query.queries, c.query.coeff)],
                      "coeff": list(c.query.coeff),
                      "shapes": [(int(q.num_columns), int(q.num_rows)) for q in c.query.queries]},
        } if c is not None else None


class MultiArrayConvertibleToSpark:
    """"""
    def __init__(self, array):
        coo = array.sparse_array.tocoo() if array is not None else None
        self.struct = {
            "data": coo.data.tolist(),
            "j": coo.col.tolist(),
            "shape": array.shape
        } if array is not None else None

    # To recreate sparse array do spar = ss.coo_matrix((data, (np.zeros(len(j), dtype=int), j)), shape=(1, np.prod(shape)).tocsr()
    # To recreate multiSparse do multiSparse(spar, shape)
    # Or multiSparse.fromJK(ma['j'], ma['data'], ma['shape']) which is a multiSparse method performing the 2 above steps

    @classmethod
    def spark_type(cls, type):
        if type == int:
            stype = T.IntegerType()
        elif type == float:
            stype = T.DoubleType()
        else:
            raise ValueError("Only integer and real types of array are supported")

        spark_type_dict = {
            "data":  (T.ArrayType(stype), True),
            "j":     (T.ArrayType(T.IntegerType()), True),
            "shape": (T.ArrayType(T.ShortType()), True)
        }

        return sparkTypeDict2StructType(spark_type_dict)


class GeounitNodeConvertibleToSpark:
    """ Should only have dicts and lists, possibly nested"""
    spark_type_dict = {
        "geocode":         (T.StringType(), False),
        "parentGeocode":   (T.StringType(), False),
        "geocodeDict":     (T.MapType(T.ShortType(), T.StringType(), False), False),
        "geolevel":        (T.StringType(), False),
        CC.WRITER_GEODICT: (T.MapType(T.ShortType(), T.StringType(), False), True),
        CC.WRITER_GEOCODE: (T.StringType(), True),
        "dp_queries":      (T.MapType(T.StringType(), DPQueryConvertibleToSparkType.spark_type, False), True),
        "unit_dp_queries": (T.MapType(T.StringType(), DPQueryConvertibleToSparkType.spark_type, False), True),
        "cons":            (T.MapType(T.StringType(), ConstraintConvertibleToSparkType.spark_type, False), True),
        "invar":           (T.MapType(T.StringType(),
                                      T.StructType([
                                          T.StructField("data", T.ArrayType(T.IntegerType())),
                                          T.StructField("shape", T.ArrayType(T.IntegerType()))
                                      ]), False), True),
        "raw":             (MultiArrayConvertibleToSpark.spark_type(int), True),
        "raw_housing":     (MultiArrayConvertibleToSpark.spark_type(int), True),
        "syn":             (MultiArrayConvertibleToSpark.spark_type(int), True),
        "unit_syn":        (MultiArrayConvertibleToSpark.spark_type(int), True),
        "syn_unrounded":   (MultiArrayConvertibleToSpark.spark_type(float), True),
        "rounder_queries": (T.ArrayType(T.MapType(T.StringType(), SumOverGroupedQuery2Spark.spark_type, True), True), True),
        "query_ordering":  (
            ## NOTE: We have fully switched to using the outer/inner pass (i.e. interleaved) optimizers (and with decomposition)
            # but other parts of code still support older optimizers, which have one nested layer of dicts less. To use those
            # one would need to implement a different struct type here, with a condition that is checked in the struct, of which
            # type of dict this is
                            T.MapType(  # Keys are CC.L2_QUERY_ORDERING, CC.L2_CONSTRAIN_TO_QUERY_ORDERING, CC.ROUNDER_QUERY_ORDERING
                                T.StringType(), T.MapType(  # Keys are outer pass numbers
                                    T.ShortType(), T.MapType(  # Keys are inner pass numbers
                                        T.ShortType(), T.ArrayType(T.StringType())
                                    )
                                )
                            ), True),
        "nmhash":          (T.BinaryType(), True),
        # "opt_dict": (T.MapType(T.StringType(), SumOverGroupedQuery2Spark.spark_type), True),  # Not tested, may not work properly
    }

    spark_type = sparkTypeDict2StructType(spark_type_dict)

    def __init__(self, node):
        self.struct = {
            "geocode":         node.geocode,
            "parentGeocode":   node.parentGeocode,
            "geocodeDict":     node.geocodeDict,
            "geolevel":        node.geolevel,
            CC.WRITER_GEODICT: node.writer_geodict,
            CC.WRITER_GEOCODE: node.writer_geocode,
            "dp_queries":      self.dict2SparkMap(node.dp_queries, DPQueryConvertibleToSparkType),
            "unit_dp_queries": self.dict2SparkMap(node.unit_dp_queries, DPQueryConvertibleToSparkType),
            "cons":            self.dict2SparkMap(node.cons, ConstraintConvertibleToSparkType),
            "invar":           self.dict2SparkMap(node.invar, None, lambda inv: {"data": inv.ravel().tolist(), "shape": inv.shape}),
            "raw":             MultiArrayConvertibleToSpark(node.raw).struct,
            "raw_housing":     MultiArrayConvertibleToSpark(node.raw_housing).struct,
            "syn":             MultiArrayConvertibleToSpark(node.syn).struct,
            "unit_syn":        MultiArrayConvertibleToSpark(node.unit_syn).struct,
            "syn_unrounded":   MultiArrayConvertibleToSpark(node.syn_unrounded).struct,
            "rounder_queries": [self.dict2SparkMap(rqdict, SumOverGroupedQuery2Spark) for rqdict in node.rounder_queries] if node.rounder_queries is not None else None,
            "query_ordering":  node.query_ordering,
            "nmhash":         node.nmhash,
            # "opt_dict":        self.dict2SparkMap(node.opt_dict, SumOverGroupedQuery2Spark)
        }

    @staticmethod
    def dict2SparkMap(d, stype_conv, cust_conv_fun=None):
        fun = cust_conv_fun if cust_conv_fun is not None else lambda v: stype_conv(v).struct
        return {k: fun(v) for k, v in d.items()} if d is not None else None

    def __repr__(self):
        return str(self.struct)


class SingleColumnGeounitNodeType:
    spark_type = T.StructType([T.StructField("GeounitNode", GeounitNodeConvertibleToSpark.spark_type)])


class TwoColumnKeyAndGeounitNodeType:
    spark_type = T.StructType([T.StructField("PartKey", T.IntegerType()), T.StructField("GeounitNode", GeounitNodeConvertibleToSpark.spark_type)])


class GeocodeRawSynConvertibleToSpark:
    spark_type_dict = {
        "geocode":         (T.StringType(), False),
        "raw":             (MultiArrayConvertibleToSpark.spark_type(int), True),
        "syn":             (MultiArrayConvertibleToSpark.spark_type(int), True),
        "raw_housing":     (MultiArrayConvertibleToSpark.spark_type(int), True),
        "unit_syn":         (MultiArrayConvertibleToSpark.spark_type(int), True),
    }

    spark_type = sparkTypeDict2StructType(spark_type_dict)

    def __init__(self, node):
        self.struct = {
            "geocode":         node.geocode,
            "raw":             MultiArrayConvertibleToSpark(node.raw).struct,
            "syn":             MultiArrayConvertibleToSpark(node.syn).struct,
            "raw_housing":     MultiArrayConvertibleToSpark(node.raw_housing).struct,
            "unit_syn":        MultiArrayConvertibleToSpark(node.unit_syn).struct,
        }