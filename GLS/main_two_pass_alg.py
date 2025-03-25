from time import time
import datetime
import os, sys, json

import subprocess
import tempfile
from operator import add

from functools import reduce
from copy import deepcopy
from scipy import sparse as ss

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "das_decennial"))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "dependencies"))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.errors import AnalysisException
import pyspark.sql.functions as F

from GLS.das_decennial.das_constants import CC
from GLS.das_decennial.programs.schema.schemas.schemamaker import SchemaMaker
from GLS.das_decennial.programs.s3cat import get_tmp
from GLS.two_pass_gls_parquet import *
from GLS.das_decennial.programs.optimization.geo_optimizer_decomp import applyFunctionToRows, combineRows


VAR_FOR_CONSTR_QUERY = 1 / 2 ** 14
FROM_ZERO_CONSTR = "from_zero_constr"


def setup_spine_structures(reader_path, geolevels, constrs_dir_name, dpquery_dir_name, query_name_suffix_len, total_invariants_geolevels, shuffle_partitions_dict):
    """
    Creates RDDs of NMFs and data structures that encode the geographic spine.
    :return levels_dict: a dictionary with format {string geolevel name : list of geocodes in the geolevel}
    :return geolevel_rdds: a dictionary with format {string geolevel name : the NMF RDD for the geolevel}, where each NMF RDD for each geolevel has row format:
    (<geocode>, {'geocode': <geocode>, query_name[0] : {'value': <query[0] noisy measurement>, 'variance': <query[0] noisy measurement>}, query_name[1] : {...}, ...})
    :return widths_dict: a dictionary with format {integer geocode fixed width : geolevel name}
    """

    spark = SparkSession.builder.getOrCreate()
    levels_dict = {}  # Maps the geolevel name to the list of geocodes in the geolevel
    geolevel_rdds = {}  # Maps geolevel name to spark DF containing NMFs for the geounits in the geolevel
    widths_dict = {}  # maps geolevel length to geolevel name

    for geolevel in geolevels:
        # Use try and except here because the Prim geolevel in DHC PR NMFs is empty, because all geounits have been bypassed:
        try:
            geolevel_rdds[geolevel] = spark.read.parquet("/".join([reader_path, geolevel + ".parquet", dpquery_dir_name])).rdd
        except AnalysisException:
            # In the case of the Prim geolevel of DHC PR NMFs, add each geounit to the geolevel, each with a single detailed query group noisy measurement with infinite variance:
            assert geolevel == "Prim" and "PR" in reader_path
            geolevel_rdds[geolevel] = (spark.read.parquet("/".join([reader_path, geolevel + ".parquet", constrs_dir_name])).rdd.map(lambda row: row["geocode"]).distinct()
                                       .map(lambda row: {"geocode": row, 'query_name': ''.join(['detailed', '_' * query_name_suffix_len]), 'value': None, 'variance': np.inf}))

        # Convert the RDD with elements given by query/geounit combinations to an alternative format, with each element corresponding to a geounit:
        geolevel_rdds[geolevel] = (geolevel_rdds[geolevel].map(lambda row: (row['geocode'], {'geocode': row['geocode'], row['query_name'][:-query_name_suffix_len]:
                                                                                             {'value': row['value'], 'variance': row['variance'], 'levels': None, FROM_ZERO_CONSTR: False}}))
                                   .reduceByKey(lambda row1, row2: {**row1, **row2}, shuffle_partitions_dict[geolevel]).persist())
        # Row format is: (geocode, {'geocode': geocode, query_name1:{'value': value, 'variance':variance, 'levels': None, FROM_ZERO_CONSTR: False}, query_name2:{...}, ...})

        # levels_dict[geolevel] is a list of each geocode in the geolevel considered in this iteration; we use the constraints file of the NMFs for this because, unlike the DPQuery file,
        # it contains all the geocodes in the geolevel:
        levels_dict[geolevel] = sorted(spark.read.parquet("/".join([reader_path, geolevel + ".parquet", constrs_dir_name]))
                                       .select("geocode").rdd.map(lambda row: row["geocode"]).distinct().collect())

        # Find the set of geocodes of the geolevel that are not in geolevel_rdds[geolevel]: 
        geocodes_subset = geolevel_rdds[geolevel].map(lambda row: row[0]).collect()
        geocodes_to_add = [str(x) for x in np.setdiff1d(levels_dict[geolevel], geocodes_subset)]

        widths_dict[len(levels_dict[geolevel][0])] = geolevel
        levels_dict[geolevel] = spark.sparkContext.broadcast({k: i for i, k in enumerate(levels_dict[geolevel])})
        # Add rows to the noisy measurements dataframe for geounits in the constraints dataframe but not in the noisy measurements dataframe:
        geolevel_rdds[geolevel] = geolevel_rdds[geolevel].union(spark.sparkContext.parallelize(geocodes_to_add)
                                                                .map(lambda row: (row, {"geocode": row, 'detailed': {'value': None, 'variance': np.inf, 'levels': None, FROM_ZERO_CONSTR: False}})))
        if geolevel != "Block":
            # In this version of the code we do not include constraints for the block geolevel:
            # TODO: include constraints at the block geolevel that included in all blocks
            constrs_rdd = (spark.read.parquet("/".join([reader_path, geolevel + ".parquet", constrs_dir_name]))
                           .rdd.map(lambda row: convert_constr_to_query(row, geolevel in total_invariants_geolevels)).reduceByKey(lambda row1, row2: {**row1, **row2}, shuffle_partitions_dict[geolevel]))
            geolevel_rdds[geolevel] = (geolevel_rdds[geolevel].join(constrs_rdd, shuffle_partitions_dict[geolevel]).map(lambda row: (row[0], {**row[1][0], **row[1][1]})))
    return levels_dict, geolevel_rdds, widths_dict


def add_pl94_hu_constrs(pl94_reader_path, schema_name, constrs_dir_name, levels_dict, geolevel_rdds, widths_dict):
    if (pl94_reader_path is None) or (schema_name != CC.SCHEMA_DHCP):
        # Do not add any constraints when an input path for PL94 NMFs is not given and/or when estimating a non-DHCP schema:
        return geolevel_rdds
    spark = SparkSession.builder.getOrCreate()
    widths = sorted(widths_dict.keys(), reverse=True)
    geoid_map = {key[-16:]: key for key in levels_dict["Block"].value.keys()}

    pl94_constrs = (spark.read.parquet("/".join([pl94_reader_path + "Block.parquet", constrs_dir_name]))
                    .filter(F.col('query_name') == "hhgq_total_ub_con").rdd
                    .map(lambda row: (geoid_map[row["geocode"][-16:]], row['value'][0]))
                    .map(lambda row: (row[0], {} if row[1] > 0 else {CC.HHGQ_HHINSTLEVELS: {'value': [0], 'variance': VAR_FOR_CONSTR_QUERY, 'levels': [0], FROM_ZERO_CONSTR: True}})).persist())
    # In this version of the code we do not include constraints for the block geolevel:
    # geolevel_rdds["Block"] = geolevel_rdds["Block"].join(pl94_constrs).map(lambda row: (row[0], {**row[1][0], **row[1][1]}))

    # Aggregate the block zero-HU constraints up to higher geolevels and join with the DHCP geounits:
    for width_ind, width in enumerate(widths[:-1]):
        par_width = widths[width_ind + 1]
        par_geolevel = widths_dict[par_width]
        pl94_constrs = pl94_constrs.map(lambda row: (row[0][:par_width], row[1])).reduceByKey(lambda row0, row1: {} if len(row0) == 0 or len(row1) == 0 else row0)
        geolevel_rdds[par_geolevel] = geolevel_rdds[par_geolevel].join(pl94_constrs).map(lambda row: (row[0], {**row[1][0], **row[1][1]}))
    return geolevel_rdds


def convert_constr_to_query(row, is_total_invariant_geolevel):
    # To keep the dimension of matrix inverses low, this codebase only approximately imposes equality constraints. Specifically, rather than inverting the KKT matrix for constrained GLS solves with
    # constraints of the form A * x = b, we include the rows of A in the coefficient matrix (/strategy matrix) with noisy measurements corresponding to the vector b and variance terms given by VAR_FOR_CONSTR_QUERY,
    # where VAR_FOR_CONSTR_QUERY is a positive value near zero. This result is that A * x \approx b, while ensuring that constraints do not increase the dimensions of any inverse matrices.
    if isinstance(row, Row):
        row = row.asDict()

    value = np.array(row["value"]).astype(np.int64)
    if is_total_invariant_geolevel and row["query_name"] in ["total_con", "pl94_con"]:
        return row["geocode"], {"total": {"value": [int(np.sum(value))], "variance": VAR_FOR_CONSTR_QUERY, "levels": None, FROM_ZERO_CONSTR: False}}
    if row["query_name"] in ["total_con", "pl94_con"] or row["sign"] == ">=":
        return row["geocode"], {}
    if row["sign"] == "<=" and np.all(value > 0):
        return row["geocode"], {}

    if row["sign"] == "<=":
        levels = np.nonzero(~value.astype(bool))[0]
        value = value[levels]
    else:
        levels = None
    query_name_list = []
    non_attr_dims = ["geocode", "query_name", "sign", "query_shape", "value", "variance", "plb", "State", "County"]
    for key in row.keys():
        if isinstance(row[key], str) and not np.any([key in non_attr_dim for non_attr_dim in non_attr_dims]) and "*" not in row[key]:
            query_name_list.append(row[key])
    query_name = " * ".join(query_name_list)
    assert np.all(np.array(value) == 0)
    return row["geocode"], {query_name: {"value": value, "variance": VAR_FOR_CONSTR_QUERY, "levels": levels, FROM_ZERO_CONSTR: True}}


def inv(array):
    """
    Since geounits without any sibling geounits have row['covar_cond_g'] given by a diagonal matrix with each diagonal element equal to infinity, this function provides a convenient way to deal with taking
    inverses of matrices of this form. Specifically, this function returns a matrix of zeros in this case, and, for the case in which the input matrix has all zero elements, this matrix returns the diagonal
    matrix with each diagonal element equal to infinity. These choice of outputs can be motivated by considering the inverse of a diagonal matrix A with each diagonal element given by the scalar
    x and taking the limit as x goes to zero or infinity.
    """

    assert array.ndim == 2 and array.shape[0] == array.shape[1]
    if np.any(np.isinf(np.diag(array))):
        assert np.all(np.isinf(np.diag(array)))
        return np.zeros(array.shape)
    if np.all(np.diag(array) == 0):
        assert np.all(array.flatten() == 0)
        return np.diag(np.full(array.shape[0], np.inf))
    return np.linalg.inv(array)


def make_nonzeros_mask_vec(schema, row):
    ncols = schema.size
    inds_vec = np.zeros(ncols)
    for query in row.keys():
        if not isinstance(row[query], dict):
            continue
        if row[query][FROM_ZERO_CONSTR]:
            query_mat = ss.csr_matrix(schema.getQuery(query).matrixRep())
            if row[query]['levels'] is not None:
                query_mat = query_mat[row[query]['levels'], :]
            inds_vec += query_mat.T.dot(np.ones(query_mat.shape[0]))

    # Return a mask vector, say x, such that x[i] == 1 if and only if the i^th detailed cell is not constrained to zero
    return (inds_vec == 0).astype(np.int8)


def make_proj_mat(combined_row_space_basis, sparse_mat):
    # See comments in init_bottom_up_pass for more information on both of the two ways this function is used:
    res_mat = combined_row_space_basis.dot(sparse_mat.T).tocsr()
    res_mat.eliminate_zeros()
    # Since each row of combined_row_space_basis is given by the sum over one or more rows of sparse_mat, each element of the i^th column of combined_row_space_basis.dot(sparse_mat.T) is either the i^th row-sum
    # of sparse_mat or zero. So, all nonzero elements of combined_row_space_basis.dot(sparse_mat.T).dot((sparse_mat.dot(sparse_mat.T)) ^ {-1}) are equal to one:
    res_mat.data = np.ones(res_mat.data.size)
    return res_mat


def init_bottom_up_pass(schema_name, row, widths0, marginalize_query, skip_matrix_save=False, bg_fanout_values=None, covar_cond_g_minus_block=None):
    # This function performs the initialization step for the bottom-up pass of the two pass algorithm.
    init_time = time()

    # Our writer and reader assume all of the following keys are in each row dictionary, so initialize them with values given by None:
    # We use the keys below often throughout this repository. In most cases the corresponding variable in the paper is obvious, but more detail may be helpful in a few cases.
    # First, to avoid referring to variance/covariance matrices using the abbreviation "var," since this might be confused with an abbreviation for "variable," we refer to all
    # such matrices using the abbreviation "covar." For example, "covar_tilde" corresponds to the matrix denoted by $Var(\tilde{\boldsymbol{\beta}}(u)$ in the paper. Second,
    # "a_mat" corresponds to the matrix denoted by $A(u)$ in the paper.
    res = {key: None for key in ['beta_hat_cond_g_minus', 'covar_cond_g_minus', 'beta_tilde', 'covar_tilde', 'a_mat', 'covar_cond_g', 'beta_hat_cond_g', 'num_siblings']}
    res["geocode"] = row["geocode"]

    # To avoid writing unnecessary arrays after this function is called, we save different arrays at the block level:
    covar_mat_name = 'covar_cond_g_minus' if len(row['geocode']) == widths0 else 'covar_cond_g'
    est_name = 'beta_hat_cond_g_minus' if len(row['geocode']) == widths0 else 'beta_hat_cond_g'
    schema = SchemaMaker.fromName(schema_name)
    nonzeros_mask = make_nonzeros_mask_vec(schema, row)

    # For the block geounits, add the "num_siblings" key to res:
    if bg_fanout_values is not None:
        par_width = len(list(bg_fanout_values.value.keys())[0])
        res["num_siblings"] = deepcopy(bg_fanout_values.value[row['geocode'][:par_width]])

    if marginalize_query is None:
        # Since we are not marginalizing, the number of detailed cells (ie, ncols) will not change:
        ncols = schema.size
    else:
        # The detailed cell counts, after marginalizing, will be the query answers of the query marginalize_query:
        mq_facs = [ss.csr_matrix(fac) for fac in schema.getQuery(marginalize_query).kronFactors()]
        mq_mat = reduce(ss.kron, mq_facs).tocsr()
        mq_mat.eliminate_zeros()
        ncols = mq_mat.shape[0]

    if row['detailed']['variance'] == np.inf:
        # We output an initial estimate (conditional only on the noisy measurements of the given geounit) for all geounits in this function; in the case of geounits that
        # are bypassed during spine optimization, define the covariance matrix so that no weight is placed on this estimate in the bottom-up pass:
        if not skip_matrix_save:
            res[covar_mat_name] = np.diag(np.full(ncols, np.inf))
        res[est_name] = np.zeros(ncols)
        return res

    # TODO: Generalize this next code block without hardcoded constants:
    if schema_name == CC.SCHEMA_DHCH_TEN_3LEV:
        assert "tenvacgq" in row.keys()
        # For the query CC.ATTR_HHTENSHORT_3LEV, the variable proj_for_query_answer below is defined as either an identity matrix, ie, when "hhtenshort_3lev" in marginalize_query,
        # or [[1,1,1]] otherwise. The following definition of row[CC.ATTR_HHTENSHORT_3LEV]['variance'] results in marg_variance being defined correctly below in both of these two cases
        row[CC.ATTR_HHTENSHORT_3LEV] = {"value":  [row["tenvacgq"]["value"][0], row["tenvacgq"]["value"][1], row["tenvacgq"]["value"][2] + row["tenvacgq"]["value"][3]],
                                        "variance": (np.array([1, 1, 2]) * row["tenvacgq"]["variance"] if "hhtenshort_3lev" in marginalize_query else 4/3 * row["tenvacgq"]["variance"]),
                                        "levels": None}
        row.pop("tenvacgq")
    # TODO: Add support for units histogram of DHCH

    wtd_strat = np.zeros((0, ncols))
    wtd_answers = np.array([])
    for query in row.keys():
        if not isinstance(row[query], dict) or row[query][FROM_ZERO_CONSTR]:
            continue

        if marginalize_query is None:
            query_mat = schema.getQuery(query).matrixRep()
            assert len(query_mat.shape) == 2
            wtd_strat = np.vstack((wtd_strat, query_mat.toarray() / np.sqrt(row[query]['variance'])))
            wtd_answers = np.concatenate((wtd_answers, np.array(row[query]['value']) / np.sqrt(row[query]['variance'])))
        else:
            query_mat = ss.csr_matrix(schema.getQuery(query).matrixRep())
            query_mat.eliminate_zeros()

            # The comments in this section make use of several properties of marginal query group matrices, which refers to a matrix composed of columns with all elements equal to zero except for one element, which
            # is equal to one. These matrices can also be decomposed into a set of smaller Kronecker factors, each of which are also composed of columns with only one nonzero element given by 1.
            query_facs = [ss.csr_matrix(fac) for fac in schema.getQuery(query).kronFactors()]
            # applyFunctionToRows([[x[0], x[1], ...]], [[y[0], y[1], ...]], combineRows)[0] provides the Kronecker factors, say [z[0], z[1], ...], of a marginal query group matrix such that, for each z[i], the
            # nonzero elements for columns A and B are in the same row if they are in the same row for either x[i] or y[i]. In other words, combined_row_space_basis below is defined so that its row space is the
            # intersection of the row spaces of query_mat (=reduce(ss.kron, query_facs)) and mq_mat (=reduce(ss.kron, mq_facs)):
            combined_facs = applyFunctionToRows([query_facs], [mq_facs], combineRows)[0]
            combined_row_space_basis = reduce(ss.kron, combined_facs).tocsr()
            combined_row_space_basis.eliminate_zeros()
            for combined_fac in combined_facs:
                nonzero_cf_levels = combined_fac.dot(np.ones(combined_fac.shape[1])) > 0
                assert np.sum(nonzero_cf_levels) == len(nonzero_cf_levels)

            # To project the row_space of query_mat onto combined_row_space_basis, we can apply the linear operator x \mapsto combined_row_space_basis query_mat^T (query_mat query_mat^T) ^ {-1} x.
            # Since query_mat is a query group matrix, query_mat query_mat^T is a diagonal matrix, so rather than directly computing this inverse, we use the following more efficient method of
            # defining this linear operator:
            proj_for_query_answer = make_proj_mat(combined_row_space_basis, query_mat)

            # Thus, noisy measurements after marginalizing (and reweighting by inverse standard deviations) are:
            answer = proj_for_query_answer.dot(np.array(row[query]['value']))
            marg_variance = proj_for_query_answer.dot(np.ones(proj_for_query_answer.shape[1])) * row[query]['variance']
            wtd_answers = np.concatenate((wtd_answers, answer / np.sqrt(marg_variance)))

            # Use a similar approach to find projection operator from row space of mq_mat to row space of combined_row_space_basis:
            strat = make_proj_mat(combined_row_space_basis, mq_mat)
            # Unlike mq_mat, at this point strat has much lower number of columns even for the DHC schemas, so convert to a dense array:
            strat = strat.toarray()
            wtd_strat = np.vstack((wtd_strat, np.diag(1. / np.sqrt(marg_variance)).dot(strat)))

    # To ensure the strategy matrix has full column rank when columns corresponding to detailed cell counts of zero have been zeroed out:
    if np.sum(nonzeros_mask) < ncols and marginalize_query is None:
        zeros_qg = np.eye(ncols)[nonzeros_mask == 0, :]
        wtd_strat = np.vstack((wtd_strat, zeros_qg / np.sqrt(VAR_FOR_CONSTR_QUERY)))
        wtd_answers = np.concatenate((wtd_answers, np.zeros(zeros_qg.shape[0])))
    elif marginalize_query is not None:
        marg_zeros_mask = mq_mat.dot(nonzeros_mask) == 0
        if np.sum(marg_zeros_mask) > 0:
            zeros_qg = np.eye(ncols)[marg_zeros_mask, :]
            wtd_strat = np.vstack((wtd_strat, zeros_qg / np.sqrt(VAR_FOR_CONSTR_QUERY)))
            wtd_answers = np.concatenate((wtd_answers, np.zeros(zeros_qg.shape[0])))

    # The count estimate and its corresponding covariance matrix defined below are derived in the section of the paper titled "Bottom-Up Pass: Initialization Step":
    res[covar_mat_name] = covar_cond_g_minus_block.value if covar_cond_g_minus_block is not None else inv(wtd_strat.T.dot(wtd_strat))
    res[est_name] = res[covar_mat_name].dot(wtd_strat.T.dot(wtd_answers))

    # For the block geounits, replace value corresponding to covar_mat_name with None:
    if skip_matrix_save:
        res[covar_mat_name] = None
    total_time = time() - init_time
    if total_time > 7:
        # If this is taking too long at the block level, it is likely numpy is not being used with an optimized BLAS/LAPACK (like MKL), which significantly
        # increases runtime. The following can be used to check the BLAS/LAPACK implementations:
        print(f"Total time in init_bottom_up_pass(): {total_time} and np.show_config():\n{np.show_config()}")
    return res


def sum_cond_g_minus(row1, row2):
    # After this reduce operation is called on all children of a given parent geounit, row["beta_hat_cond_g_minus"] will be equal to the independent estimate
    # of the state vector and row["covar_cond_g_minus"] will be equal to its variance matrix. This function is called two times in the two-pass GLS algorithm,
    # as described in the sections titled "Bottom-Up Pass: Recursion Step" and "Top-Down Pass: Recursion Step":
    if row1["covar_cond_g_minus"] is not None:
        # For the block geolevel, we do not save the matrix "covar_cond_g_minus" in the geounit dict, so only sum these matrices in all other geolevels:
        row1["covar_cond_g_minus"] = row1['covar_cond_g_minus'] + row2['covar_cond_g_minus']
    row1["beta_hat_cond_g_minus"] = row1['beta_hat_cond_g_minus'] + row2['beta_hat_cond_g_minus']
    return row1


def bottom_up_pass(rows, widths_last, covar_cond_g_minus_block, inv_covar_cond_g_minus):
    par_ind = np.argmin([len(row['geocode']) for row in rows])
    par_row = rows[par_ind]
    child_row = rows[1 - par_ind]

    if child_row["covar_cond_g_minus"] is None:
        # This implies the child is in the block geolevel; we infer whether child_row["covar_cond_g_minus"] would have infinite elements on the diagonal,
        # if it would have been saved explicitly, using row["num_siblings"]. If this is not the case, we define child_covar_cond_g_minus as the sum of this
        # covariance matrix over each child geounit:
        child_covar_cond_g_minus = np.diag(np.full(child_row["beta_hat_cond_g_minus"].size, np.inf)) if child_row["num_siblings"] == 1 else covar_cond_g_minus_block.value * child_row["num_siblings"]
        inv_covar_cond_children_minus = np.zeros(child_covar_cond_g_minus.shape) if child_row["num_siblings"] == 1 else inv_covar_cond_g_minus / child_row["num_siblings"]
    else:
        # In all other cases, we can use the saved matrix directly:
        child_covar_cond_g_minus = child_row["covar_cond_g_minus"]
        # At this point child_covar_cond_g_minus is the sum of the covariance matrices conditional on g over each child geounit g:
        inv_covar_cond_children_minus = inv(child_covar_cond_g_minus)

    if np.any(np.isinf(np.diag(par_row['covar_cond_g']))) and np.any(np.isinf(np.diag(child_covar_cond_g_minus))):
        # Define the covariance matrix and estimate as those of beta_hat_cond_g
        par_row["covar_cond_g_minus"] = deepcopy(par_row['covar_cond_g'])
        par_row["beta_hat_cond_g_minus"] = deepcopy(par_row["beta_hat_cond_g"])
        return par_row['geocode'], par_row

    inv_covar_cond_g = inv(par_row['covar_cond_g'])
    par_row["covar_cond_g_minus"] = inv(inv_covar_cond_g + inv_covar_cond_children_minus)
    par_row["beta_hat_cond_g_minus"] = par_row["covar_cond_g_minus"].dot(inv_covar_cond_g.dot(par_row["beta_hat_cond_g"]) +
                                                                         inv_covar_cond_children_minus.dot(child_row['beta_hat_cond_g_minus']))
    if len(par_row['geocode']) == widths_last:
        # In the case of the root geounit, initialize the top-down pass here:
        par_row["covar_tilde"] = deepcopy(par_row["covar_cond_g_minus"])
        par_row["beta_tilde"] = deepcopy(par_row["beta_hat_cond_g_minus"])
    return par_row['geocode'], par_row


def create_top_down_child_est(child, child_sums, par_row, skip_matrix_save=False):
    if skip_matrix_save:
        # Child is a block in this case, so rather than deriving a_mat using row["covar_cond_g_minus"] as is done below, we can use the following simplification instead,
        # which follows from the matrix row["covar_cond_g_minus"] being the same for each sibling geounit of the block child:
        # a_mat = np.eye(child["beta_hat_cond_g_minus"].size) / row["num_siblings"]
        # So, rather than multiplying directly by a_mat, we can just use:
        child["beta_tilde"] = child["beta_hat_cond_g_minus"] + (par_row["beta_tilde"] - child_sums["sum_beta_hat_cond_g_minus"]) / child["num_siblings"]
        for key in ['covar_cond_g_minus', 'covar_tilde', 'a_mat']:
            child[key] = None
    else:
        # For all other geolevels, save child["covar_tilde"] and child["a_mat"]
        child["a_mat"] = child["covar_cond_g_minus"].dot(child_sums["sum_covar_cond_g_minus_inv"])
        child["beta_tilde"] = child["beta_hat_cond_g_minus"] + child["a_mat"].dot(par_row["beta_tilde"] - child_sums["sum_beta_hat_cond_g_minus"])
        child["covar_tilde"] = child["covar_cond_g_minus"] + child["a_mat"].dot(par_row["covar_tilde"]).dot(child["a_mat"].T) - child["a_mat"].dot(child["covar_cond_g_minus"])
    for key in ['beta_hat_cond_g_minus', 'covar_cond_g', 'beta_hat_cond_g']:
        child[key] = None
    return child['geocode'], child


def convert_to_geolevel_rdd(row, skip_matrix_save):
    cs = {}
    child = {}
    par = {}
    # Vector columns:
    child['beta_hat_cond_g_minus'] = None if row["beta_hat_cond_g_minus_da_ch"] is None else np.array(row["beta_hat_cond_g_minus_da_ch"])
    par['beta_tilde'] = np.array(row['beta_tilde_da'])
    cs["sum_beta_hat_cond_g_minus"] = None if row["sum_beta_hat_cond_g_minus_da"] is None else np.array(row["sum_beta_hat_cond_g_minus_da"])

    # The remaining arrays all encode symmetric matrices:
    if not skip_matrix_save:
        child["covar_cond_g_minus"] = from_bytes_to_symmetric_mat(row["covar_cond_g_minus_da_ch"])
        par["covar_tilde"] = from_bytes_to_symmetric_mat(row["covar_tilde_da"])
        cs["sum_covar_cond_g_minus_inv"] = from_bytes_to_symmetric_mat(row["sum_covar_cond_g_minus_inv_da"])

    child["geocode"] = row["geocode_ch"]
    child["num_siblings"] = row["num_siblings_ch"]
    return create_top_down_child_est(child, cs, par, skip_matrix_save)


def find_block_with_siblings(block_geocodes, parent_width):
    par_last = block_geocodes[0][:parent_width]
    for block_geocode in block_geocodes[1:]:
        par_cur = block_geocode[:parent_width]
        if par_last == par_cur:
            return block_geocode
        par_last = par_cur
    assert False, "Failed to find a block with siblings"


def two_pass_method(schema_name, levels_dict, geolevel_rdds, widths_dict, writer_path, partition_size, marginalize_query, shuffle_partitions_dict):
    # This is the main function of the two-pass estimation approach that is outlined in Algorithm 1 in the paper https://arxiv.org/abs/2404.13164
    spark = SparkSession.builder.getOrCreate()

    widths = sorted(widths_dict.keys(), reverse=True)
    checkpoint_dir = writer_path + "checkpoint/bottom_up_pass/"

    # Perform the bottom-up pass, which leverages information contained in the NMFs of lower geolevels to increase accuracy of estimates in higher geolevels:
    for width_ind, width in enumerate(widths[:-1]):
        geolevel = widths_dict[width]
        par_width = widths[width_ind + 1]
        par_geolevel = widths_dict[par_width]

        # To reload previously estimated bottom-up pass nodes rather than rerunning the bottom-up pass, uncomment the following block:
        # if width_ind == 0:
        #     representative_block = read_parquet_gls_two_pass_node(writer_path + "gls_est/representative_Block_gls.ext")
        #     covar_cond_g_minus_block = spark.sparkContext.broadcast(representative_block.collect()[0][1]["covar_cond_g_minus"])
        #     geolevel_rdds[geolevel] = read_parquet_gls_two_pass_node(checkpoint_dir + geolevel + "_bottom_up.ext")
        # checkpt_path = writer_path + "gls_est/" + par_geolevel + "_gls.ext" if par_width == widths[-1] else checkpoint_dir + par_geolevel + "_bottom_up.ext"
        # geolevel_rdds[par_geolevel] = read_parquet_gls_two_pass_node(checkpt_path)
        # continue

        if width_ind == 0:
            # Find block "covar_cond_g_minus" matrix for a representative block, save it for later use in est_cis.py, and then collect it:
            representative_block_code = find_block_with_siblings(list(levels_dict[geolevel].value.keys()), par_width)
            representative_block = (geolevel_rdds[geolevel].filter(lambda row: row[0] == representative_block_code)
                                    .map(lambda row: ("9" * len(row[0]), init_bottom_up_pass(schema_name, row[1], widths[0], marginalize_query))))
            representative_block = checkpoint(representative_block, levels_dict[geolevel], writer_path + "gls_est/representative_Block_gls.ext",
                                              repartition=False, partition_size=partition_size)
            covar_cond_g_minus_block = spark.sparkContext.broadcast(representative_block.collect()[0][1]["covar_cond_g_minus"])

            # For each block dictionary, add a scalar value (with key "num_siblings") that provides the number of siblings geounits of the block, and
            # also estimate and save \beta(g | g-) for each block:
            bg_fanout_values = spark.sparkContext.broadcast(geolevel_rdds[geolevel].map(lambda row: (row[0][:par_width], 1))
                                                            .reduceByKey(add).collectAsMap())
            geolevel_rdds[geolevel] = geolevel_rdds[geolevel].map(lambda row: (row[0], init_bottom_up_pass(schema_name, row[1], widths[0], marginalize_query,
                                                                                                           skip_matrix_save=True, bg_fanout_values=bg_fanout_values,
                                                                                                           covar_cond_g_minus_block=covar_cond_g_minus_block)))
            geolevel_rdds[geolevel] = checkpoint(geolevel_rdds[geolevel], levels_dict[geolevel], checkpoint_dir + geolevel + "_bottom_up.ext",
                                                 repartition=True, partition_size=partition_size).unpersist()

        # Follow a similar route as topdown_engine.py and map each parent geocode to an integer index and each child geocode to its
        # parent integer index, since this tends to be faster than alternatives:
        par_rdd = geolevel_rdds[par_geolevel].repartition(shuffle_partitions_dict[par_geolevel]).map(lambda row: (levels_dict[par_geolevel].value[row[0]],
                                                            init_bottom_up_pass(schema_name, row[1], widths[0], marginalize_query)))

        children_summed_rdd = (geolevel_rdds[geolevel].map(lambda row: (levels_dict[par_geolevel].value[row[0][:par_width]], row[1]))
                               .reduceByKey(lambda row1, row2: sum_cond_g_minus(row1, row2), shuffle_partitions_dict[par_geolevel]))
        grouped = children_summed_rdd.union(par_rdd).groupByKey()

        inv_covar_cond_g_minus = inv(covar_cond_g_minus_block.value) if geolevel == "Block" else None
        geolevel_rdds[par_geolevel] = grouped.map(lambda rows: bottom_up_pass(tuple(rows[1]), widths[-1], covar_cond_g_minus_block, inv_covar_cond_g_minus))

        # In all cases other than the root geolevel, write the parent geolevel rdd to a checkpoint directory. In the case of the root
        # geolevel write to/read from the final output location instead:
        checkpt_path = writer_path + "gls_est/" + par_geolevel + "_gls.ext" if par_width == widths[-1] else checkpoint_dir + par_geolevel + "_bottom_up.ext"
        geolevel_rdds[par_geolevel] = checkpoint(geolevel_rdds[par_geolevel], levels_dict[par_geolevel],
                                                 checkpt_path, repartition=True, partition_size=partition_size).unpersist()
        print(f"\nIn two_pass_method(), bottom-up pass complete for {par_geolevel} geolevel at {datetime.datetime.now().isoformat()}\n")

        # The following RDDs are no longer needed, so delete them to free up disk space if needed after garbage collection:
        del grouped, children_summed_rdd, par_rdd

    checkpoint_dir_td = writer_path + "checkpoint/top_down_pass/"
    widths_rev = list(reversed(widths))
    for width_ind, width in enumerate(widths_rev[:-1]):
        geolevel = widths_dict[width]
        child_width = widths_rev[width_ind + 1]
        child_geolevel = widths_dict[child_width]

        # To reload previously estimated top-down pass nodes other than those in certain geolevels, eg, other than the Block and Block_Group geolevels, use:
        # if child_geolevel not in ["Block", "Block_Group"]:
        #     geolevel_rdds[child_geolevel] = read_parquet_gls_two_pass_node(writer_path + "gls_est/" + child_geolevel + "_gls.ext")
        #     continue

        est_and_cov_sums_and_par = (geolevel_rdds[child_geolevel].repartition(shuffle_partitions_dict[child_geolevel])
                                    .map(lambda row: (levels_dict[geolevel].value[row[0][:width]], row[1]))
                                    .reduceByKey(lambda row1, row2: sum_cond_g_minus(row1, row2), shuffle_partitions_dict[child_geolevel])
                                    .repartition(shuffle_partitions_dict[child_geolevel]))
        if child_geolevel == "Block":
            est_and_cov_sums_and_par = (est_and_cov_sums_and_par.map(lambda row: (row[0], {"geocode": row[1]["geocode"][:width], "sum_covar_cond_g_minus_inv": None,
                                                                                           "sum_beta_hat_cond_g_minus": row[1]["beta_hat_cond_g_minus"]})))
        else:
            est_and_cov_sums_and_par = (est_and_cov_sums_and_par.map(lambda row: (row[0], {"geocode": row[1]["geocode"][:width], "sum_covar_cond_g_minus_inv": inv(row[1]["covar_cond_g_minus"]),
                                                                                           "sum_beta_hat_cond_g_minus": row[1]["beta_hat_cond_g_minus"]})))

        est_and_cov_sums_and_par_df = checkpoint_df(est_and_cov_sums_and_par, checkpoint_dir_td + geolevel + "_est_and_cov_sums_and_par_df.ext", spark_type_class=GLSTwoPassSummedNode)

        ch_cols_to_select = ["beta_hat_cond_g_minus_da", "geocode", "num_siblings"]
        remaining_cols_to_select = ["sum_beta_hat_cond_g_minus_da", "beta_tilde_da", "geocode"]
        skip_matrix_save = child_geolevel == "Block"
        if not skip_matrix_save:
            ch_cols_to_select.append("covar_cond_g_minus_da")
            remaining_cols_to_select.extend(["covar_tilde_da", "sum_covar_cond_g_minus_inv_da"])
        join_width = int((width + child_width) / 2)
        child_geolevel_df = (spark.read.parquet(checkpoint_dir + child_geolevel + "_bottom_up.ext")
                             .select([F.col(c).alias(c + '_ch') for c in ch_cols_to_select]).withColumn("join_code", F.substring("geocode_ch", 1, join_width))
                             .repartition(shuffle_partitions_dict[child_geolevel], "join_code"))

        par_geolevel_df = spark.read.parquet(writer_path + "gls_est/" + geolevel + "_gls.ext")
        est_and_cov_sums_and_par_df = est_and_cov_sums_and_par_df.join(par_geolevel_df, "geocode").select(remaining_cols_to_select)

        par_to_join_codes = {str(k): [] for k in np.unique([s[:width] for s in levels_dict[child_geolevel].value.keys()])}
        for child_geocode in levels_dict[child_geolevel].value.keys():
            par_to_join_codes[child_geocode[:width]].append(child_geocode[:join_width])
        par_to_children_df = spark.createDataFrame([{"geocode": k, "join_code": [str(vi) for vi in np.unique(v)]} for k, v in par_to_join_codes.items()])

        est_and_cov_sums_and_par_df = (est_and_cov_sums_and_par_df.join(par_to_children_df, "geocode").repartition(shuffle_partitions_dict[child_geolevel], "geocode")
                                       .select([F.explode("join_code").alias("join_code")] + remaining_cols_to_select)
                                       .repartition(shuffle_partitions_dict[child_geolevel], "join_code"))

        child_geolevel_df = est_and_cov_sums_and_par_df.join(child_geolevel_df, "join_code").repartition(shuffle_partitions_dict[child_geolevel], "geocode_ch")

        child_geolevel_df.write.mode("overwrite").parquet(checkpoint_dir_td + geolevel + "_child_geolevel_df.ext", compression="zstd")
        child_geolevel_df = spark.read.parquet(checkpoint_dir_td + geolevel + "_child_geolevel_df.ext")

        geolevel_rdds[child_geolevel] = child_geolevel_df.rdd.map(lambda row: convert_to_geolevel_rdd(row, skip_matrix_save))
        geolevel_rdds[child_geolevel] = checkpoint(geolevel_rdds[child_geolevel], levels_dict[child_geolevel],
                                                   writer_path + "gls_est/" + child_geolevel + "_gls.ext", repartition=False, partition_size=partition_size)
        print(f"\nIn two_pass_method(), top-down pass complete for {child_geolevel} geolevel at {datetime.datetime.now().isoformat()}\n")

        # The following RDDs are no longer needed, so delete them to free up disk space after garbage collection:
        del child_geolevel_df, est_and_cov_sums_and_par_df, par_geolevel_df, est_and_cov_sums_and_par
        geolevel_rdds[geolevel] = None


def run_two_pass_estimate(reader_path, geolevels, constrs_dir_name, dpquery_dir_name, partition_size, query_name_suffix_len, schema_name, writer_path,
                          total_invariants_geolevels, marginalize_query, pl94_reader_path, shuffle_partitions_dict=None):
    if shuffle_partitions_dict is None:
        shuffle_partitions_dict = {geolevel: None for geolevel in geolevels}
    print(f"\n\nrun_two_pass_estimate() called at {datetime.datetime.now().isoformat()}\n\n")
    levels_dict, geolevel_rdds, widths_dict = setup_spine_structures(reader_path, geolevels, constrs_dir_name, dpquery_dir_name,
                                                                     query_name_suffix_len, total_invariants_geolevels, shuffle_partitions_dict)
    with tempfile.NamedTemporaryFile(dir=get_tmp(), mode='wb') as tf:
        with open(tf.name, 'w') as txtfile:
            txtfile.write(json.dumps(widths_dict))
        subprocess.check_call(['aws', 's3', 'cp', '--quiet', '--no-progress', tf.name, writer_path + "gls_est/" + "widths_dict.json"])
    print(f"\n\nIn run_two_pass_estimate(), setup_spine_structures() complete at {datetime.datetime.now().isoformat()}\n\n")

    #geolevel_rdds = add_pl94_hu_constrs(pl94_reader_path, schema_name, constrs_dir_name, levels_dict, geolevel_rdds, widths_dict)
    #print(f"\n\nIn run_two_pass_estimate(), add_pl94_hu_constrs() complete at {datetime.datetime.now().isoformat()}\n\n")

    two_pass_method(schema_name, levels_dict, geolevel_rdds, widths_dict, writer_path, partition_size, marginalize_query, shuffle_partitions_dict)
    print(f"\n\nIn run_two_pass_estimate(), two_pass_method() complete at {datetime.datetime.now().isoformat()}\n\n")
