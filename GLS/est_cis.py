from functools import reduce
import subprocess
from copy import deepcopy
import os, sys, json
import tempfile
from time import time
from itertools import groupby
import numpy as np
from scipy import sparse as ss
from scipy.stats import norm as gaussian

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "das_decennial"))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "dependencies"))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.functions as F

from GLS.das_decennial.programs.schema.schemas.schemamaker import SchemaMaker
from GLS.das_decennial.programs.s3cat import get_tmp
from GLS.two_pass_gls_parquet import EntityAndQueryCI, write_parquet, read_parquet_gls_two_pass_node, checkpoint, GLSEntityEst, read_parquet_gls_entity_est, GLSTwoPassNode
from GLS.das_decennial.programs.optimization.geo_optimizer_decomp import applyFunctionToRows, combineRows


def find_coarsest_geounits(geocode_rdd, widths_dict):
    """
    This function returns an RDD with each row formatted as (geographic_entity[i], tuple_of_geocodes[i]) = (geographic_entity[i], (geounit_geocode[i,1], geounit_geocode[i,2], ...)), where
    tuple_of_geocodes is the set of geocodes of geounits such that no parent of these geounits is fully contained in geographic_entity[i] and each geounit_geocode[i,k] is fully contained
    in geographic_entity[i].

    geocode_rdd is an RDD with each row formatted as (block_id[i], geographic_entity_id[i])
    widths_dict is a dictionary with format {integer geocode width : string geolevel name}
    """

    widths = sorted(widths_dict.keys(), reverse=True)
    geocode_list = sorted(geocode_rdd.collect(), key=lambda x: x[0])

    def map_for_grouped(par_geocode, child_and_entity_geocodes):
        # Ouput format is [(geounit_geocode[0], entity_geocode[0]), (geounit_geocode[1], entity_geocode[1]), ...]
        entity_geocodes = tuple(set(x[1] for x in child_and_entity_geocodes))
        if len(entity_geocodes) > 1:
            return child_and_entity_geocodes
        return [(par_geocode, entity_geocodes[0])]

    init_time = time()
    for k, par_width in enumerate(widths[1:]):
        geocode_list = [map_for_grouped(k, tuple(v)) for k, v in groupby(geocode_list, key=lambda x:x[0][:par_width])]
        geocode_list = [rowij for rowi in geocode_list for rowij in rowi]
    print(f'\n\n Iterations in find_coarsest_geounits are complete and runtime was {time()-init_time} seconds.\n\n')

    spark = SparkSession.builder.getOrCreate()
    geocode_rdd_to_use = spark.sparkContext.parallelize([(x[1], x[0]) for x in geocode_list])
    return geocode_rdd_to_use.groupByKey()


def make_custom_geocode_rdds(geolevel, two_pass_rdd):
    # For some geographic entity geolevels, ie, Block, US, and State, it is more efficient to use the geocode format used by DAS to create geocode_rdds[geolevel]:
    if geolevel == "Block":
        # The DAS spine blocks are equal to tabulation blocks:
        return two_pass_rdd.map(lambda row: (row[0][-16:], (row[0],)))
    if geolevel == "US":
        # The US geounit in the DAS spine is equal to the US geounit in the standard census spine:
        assert two_pass_rdd.count() == 1
        return two_pass_rdd.map(lambda row: ('', ('',)))
    # The state geounits in the DAS spine have a geocode format given by ['0' if the geounit is not an AIAN region within a state and '1' otherwise][2 digit state FIPS code]:
    assert geolevel == "State"
    return two_pass_rdd.map(lambda row: (row[0][1:], row[0])).groupByKey().map(lambda row: (row[0], tuple(row[1])))


def setup_inputs(reader_path, tabulation_spine_geodict, geographic_entity_geolevels, nmf_reader_path, constrs_dir_name):
    """
    Creates RDDs containing the state variables of the two pass algorithm and data structures that encode some information from the geographic spine.
    :return widths_dict: a dictionary with format {integer geocode fixed width : geolevel name}
    :return two_pass_rdd: each row is formatted as (geocode[i], {<state variable name> : <state variable of geounit i>, ...}
    :return geocode_rdds: A dictionary with format {<string geographic entity geolevel name> : <spark geocode RDD for geographic entity geolevel>},
    where <spark geocode RDD for geographic entity geolevel> has a row format given by (<geographic entity geocode>, (geounit_geocode[0], geounit_geocode[1], ...))
    and each geounit_geocode[k] is the geocode of a geounit within the given geographic entity
    """
    spark = SparkSession.builder.getOrCreate()

    with tempfile.NamedTemporaryFile(dir=get_tmp(), mode='wb') as tf:
        subprocess.check_call(['aws', 's3', 'cp', '--quiet', '--no-progress', reader_path + "widths_dict.json", tf.name])
        with open(tf.name, 'r', newline='') as txtfile:
            widths_dict = {int(k): v for k, v in json.loads(txtfile.read()).items()}
        print(f"\n\nwidths_dict is: {widths_dict}\n\n")

    crosswalk = (spark.read.parquet("/".join([nmf_reader_path, "Block.parquet", constrs_dir_name])).select("geocode").rdd.map(lambda row: row["geocode"])
                 .distinct().map(lambda row: {**{geolevel: row[-16:][:width] for width, geolevel in tabulation_spine_geodict.items()}, **{'Block': row}}))
    # Row format for block k is: {<string geolevel 0 name> : <string geocode of geounit in geolevel 0 containing block k>,
    # <string geolevel 1 name> : <string geocode of geounit in geolevel 1 containing block k>...}

    # This function can also be modified to support finding entity estimates for arbitrary geographic entities. For example, to find the balance of counties (ie, the subsets of counties that are outside of incorporated/unincorporated places), the definition of the 'crosswalk' RDD above can be changed to:
    # grfc = spark.read.csv("<insert grfc path>", sep='|', header=True).rdd.map(lambda row: (row["TABBLKST"] + row["TABBLKCOU"] + row["TABTRACTCE"] + row["TABBLKGRPCE"] + row["TABBLK"], (row["TABBLKST"] + row["TABBLKCOU"]) if row["PLACEFP"] == "99999" else "99999"))
    # crosswalk = crosswalk.map(lambda row: (row["Block"][-16:], row)).join(grfc).map(lambda row: {**row[1][0], **{"BAL": row[1][1]}})

    widths_dict_rev = {val: key for key, val in widths_dict.items()}
    geocode_rdds = {}
    for geolevel in geographic_entity_geolevels:
        if geolevel in ['US', 'State', 'Block']:
            # In these cases we use a custom function to find the highest geounits in the spine that can be added together to create the off-spine entity:
            geocode_rdds[geolevel] = make_custom_geocode_rdds(geolevel, crosswalk.map(lambda row: (row['Block'][:widths_dict_rev[geolevel]],)).distinct())
        else:
            geocode_rdds[geolevel] = crosswalk.map(lambda row: (row["Block"], row[geolevel]))
            geocode_rdds[geolevel] = find_coarsest_geounits(geocode_rdds[geolevel], widths_dict)
        # Row format of geocode_rdds[geolevel] is: (geographic_entity, (geounit_geocode[0], geounit_geocode[1], ...)). The geographic extent of the geounits corresponding to the geocodes in the second element of this tuple are mutually exclusive, and, when aggregated together, form the geographic extent of the geographic entity. This script uses the concept of this set of geounits extensively; its counterpart in the paper is the data structure denoted by H after the first comment in Algorithm 3.

    representative_block = read_parquet_gls_two_pass_node(reader_path + "representative_Block_gls.ext")
    covar_cond_g_minus_block = spark.sparkContext.broadcast(representative_block.collect()[0][1]["covar_cond_g_minus"])

    # Continuing the example described above, on how to modify crosswalk to compute the CIs for balance of counties, note that this would be a good place to remove the element of geocode_rdds["balance_of_counties"] corresponding to the blocks that are not in a balance of county, using:
    # geocode_rdds["balance_of_counties"] = geocode_rdds["balance_of_counties"].filter(lambda row: row[0] != "99999")
    return widths_dict, geocode_rdds, covar_cond_g_minus_block


def find_ancestors(widths, geounit):
    # Provides the geocodes of each ancestor of geounit in the geographic spine, including the input geocode itself
    return [geounit[:width] for width in widths if width <= len(geounit)]


def find_shortest_path(widths_dict, geounit1, geounit2):
    # Provides the unique shortest path from geounit1 to geounit2 on the geographic spine
    widths = sorted(widths_dict.keys(), reverse=True)
    ancestors1 = find_ancestors(widths, geounit1)
    ancestors2 = find_ancestors(widths, geounit2)

    ancestors1_in_ancestors2 = np.isin(ancestors1, ancestors2)
    # The following should always hold because the unique root node is in ancestors1 and ancestors2:
    assert np.any(ancestors1_in_ancestors2)
    common_ancestor_ind = np.argmax(ancestors1_in_ancestors2)

    ancestor_width = len(ancestors1[common_ancestor_ind])
    shortest_path = ancestors1[:common_ancestor_ind + 1]
    shortest_path.extend(reversed([x for x in ancestors2 if len(x) > ancestor_width]))
    return shortest_path


def find_geounits_on_shortest_paths(row, widths_dict):
    # Given the set of geocodes in row[1], this function compiles the geocodes of geounits on shortest paths between any pair of elements of row[1]
    geographic_entity, geounit_geocodes = row
    geounit_geocodes = list(geounit_geocodes)
    all_geocodes = deepcopy(geounit_geocodes)
    if len(geounit_geocodes) > 1:
        for k, geocode1 in enumerate(geounit_geocodes[:-1]):
            for geocode2 in geounit_geocodes[k + 1:]:
                shortest_path = find_shortest_path(widths_dict, geocode1, geocode2)
                all_geocodes.extend(shortest_path)
    all_geocodes = list(set(all_geocodes))
    return [Row(geocode=geocode, geographic_entity=geographic_entity, geounits_in_geographic_entity=geounit_geocodes) for geocode in all_geocodes]


def find_covar_plus_covar_transpose(widths_dict, geocode1, geocode2, two_pass_dicts):
    # Using the notation from the paper, let C := Cov(\tilde{\beta}(u), Cov(\tilde{\beta}(v)), where u,v are the geounits with geocodes given by geocode1, geocode2. This function returns C + C^T
    # To connect this function with the pseudocode in the paper, ie, https://arxiv.org/abs/2404.13164, Algorithm 2 describes the method we use to compute the matrix C.
    shortest_path = find_shortest_path(widths_dict, geocode1, geocode2)
    path_geocode_widths = [len(geocode) for geocode in shortest_path]
    min_width = min(path_geocode_widths)

    if len(shortest_path[0]) == min_width or len(shortest_path[-1]) == min_width:
        n_cells = len(two_pass_dicts[geocode1]['beta_tilde'])
        covar = np.eye(n_cells)
        # In this case, geounit1 is a direct descendant of geounit2 or geounit2 is a direct descendant of geounit1
        # First we will find Cov(<descendant geounit>, <ancestor geounit>):
        if len(shortest_path[0]) == min_width:
            shortest_path = list(reversed(shortest_path))

        # Note that two_pass_dicts[geocode]['a_mat'] corresponds to the matrix denoted by $A(u)$ in the paper and two_pass_dicts[shortest_path[-1]]['covar_tilde'] corresponds to the matrix denoted by $Var(\tilde{\boldsymbol{\beta}}(u)$ in the paper
        for geocode in shortest_path[:-1]:
            covar = covar.dot(two_pass_dicts[geocode]['a_mat'])
        covar = covar.dot(two_pass_dicts[shortest_path[-1]]['covar_tilde'])
        return covar + covar.T

    # Find the covariance between the two direct children of the closest common ancestor of geounit1 and geounit2:
    ancestor_ind = np.argmin(path_geocode_widths)
    ancestor = shortest_path[ancestor_ind]
    c1 = shortest_path[ancestor_ind - 1]
    c2 = shortest_path[ancestor_ind + 1]
    covar = two_pass_dicts[c1]['a_mat'].dot(two_pass_dicts[ancestor]['covar_tilde']).dot(two_pass_dicts[c2]['a_mat'].T) - two_pass_dicts[c1]['a_mat'].dot(two_pass_dicts[c2]['covar_cond_g_minus'].T)

    if ancestor_ind - 1 > 0:
        # In this case, there are one or more descendants of c1 in shortest_path
        for geocode in reversed(shortest_path[:ancestor_ind - 1]):
            covar = two_pass_dicts[geocode]['a_mat'].dot(covar)
    if ancestor_ind + 1 < len(shortest_path) - 1:
        # In this case, there are one or more descendants of c2 in shortest_path
        for geocode in shortest_path[ancestor_ind + 2:]:
            covar = covar.dot(two_pass_dicts[geocode]['a_mat'].T)
    return covar + covar.T


def find_detailed_cell_covar(row, widths_dict, covar_cond_g_minus_block, return_sum_a_mats=False):
    # This function computes the vector of detailed cell histogram estimates for the input geographic entity, along with the covariance matrix of this estimate,
    # which are denoted by entity_estimate and covar below, respectively. Note that this is a slight variation from the route used in the paper (ie,
    # https://arxiv.org/abs/2404.13164), since the paper describes how to find the variance of a single query answer estimate instead of the variances of several
    # query answer estimates using the covariance matrix of the detailed cell histogram counts. For each query we consider with query matrix denoted by query_mat,
    # we find the variance of the query answer estimate in compute_cis(.) using query_mat.dot(covar).dot(query_mat.T)

    # Format of input row is: (geographic_entity, ((geounits_in_geographic_entity, two_pass_dict[0]), (geounits_in_geographic_entity, two_pass_dict[1]), ...))

    geographic_entity = row[0]
    geounits_in_geographic_entity = tuple(tuple(row[1])[0])[0]

    two_pass_dicts = {x[1]['geocode']: x[1] for x in row[1]}
    del row
    print(f"\nFor geographic_entity={geographic_entity}, len(two_pass_dicts)={len(two_pass_dicts)}, and geounits_in_geographic_entity={geounits_in_geographic_entity}\n")

    # Each value of two_pass_dicts is a dictionary with keys:
    # ['geocode', 'covar_cond_g', 'beta_hat_cond_g', 'beta_hat_cond_g_minus', 'a_mat', 'covar_tilde', 'covar_cond_g_minus', 'beta_tilde', 'num_siblings']

    # Recompute and add the keys for "a_mat" and "covar_cond_g_minus" to the two_pass_dict objects of block geounits:
    for geocode, tpd in two_pass_dicts.items():
        if ("a_mat" not in tpd.keys() or tpd["a_mat"] is None) and widths_dict[len(geocode)] == "Block":
            assert "covar_cond_g_minus" not in tpd.keys() or tpd["covar_cond_g_minus"] is None
            # Note that, if n denotes the number of siblings of block geounit c, then $A(c) := covar_cond_g_minus_block (covar_cond_g_minus_block * n) ^ {-1} = I / n.$
            tpd.update({"a_mat": np.eye(tpd["beta_tilde"].size) / tpd["num_siblings"], "covar_cond_g_minus": covar_cond_g_minus_block.value})

    # Initialize covariance matrix and entity estimate:
    n_cells = len(two_pass_dicts[geounits_in_geographic_entity[0]]['beta_tilde'])
    covar = np.zeros((n_cells, n_cells))
    entity_estimate = np.zeros(n_cells)
    sum_a_mats = np.zeros((n_cells, n_cells))

    for k, geocode1 in enumerate(geounits_in_geographic_entity):
        # To find the covariance more efficiently we only iterate over geocode2 in geounits_in_geographic_entity[k:]; this is possible because the covariance
        # of the estimates of geocode1 and geocode2 is equal to the transpose of that of geocode2 and geocode1. The function find_covar_plus_covar_transpose
        # returns the sum of the covariance matrix and its transpose:
        for geocode2 in geounits_in_geographic_entity[k:]:
            if geocode1 == geocode2:
                covar += two_pass_dicts[geocode1]['covar_tilde']
                entity_estimate += two_pass_dicts[geocode1]['beta_tilde']
                print(f"\n In find_detailed_cell_covar for geographic_entity = {geographic_entity}, geounits_in_geographic_entity is {geounits_in_geographic_entity}, geocode1 is {geocode1}\n")
                if return_sum_a_mats:
                    sum_a_mats += two_pass_dicts[geocode1]['a_mat']
            else:
                covar += find_covar_plus_covar_transpose(widths_dict, geocode1, geocode2, two_pass_dicts)
    if return_sum_a_mats:
        return geographic_entity, entity_estimate, covar, sum_a_mats
    return geographic_entity, entity_estimate, covar


def find_detailed_cell_covar_bottom_up(row, widths_dict, covar_cond_g_minus_block):
    # This function is similar to find_detailed_cell_covar(...) above, with modifications to support aggregating the children of each parent geounit that are
    # within the geographic entities together. Currently this function is not used, but we plan on adding future functionality that will depend on this function;
    # see the section of the paper (ie, https://arxiv.org/abs/2404.13164) on "Computational Considerations" for more detail.

    # Format of input row is: ((geographic_entity, parent), (two_pass_dict[0], two_pass_dict[1], ...))

    geographic_entity, parent = tuple(row[0])
    geounits_in_geographic_entity = tuple(row_i["geocode"] for row_i in row[1] if row_i["geocode"] != parent)
    if len(geounits_in_geographic_entity) == 0:
        assert row[1][0]['geocode'] == parent
        return parent, row[1][0]

    geographic_entity, entity_estimate, covar, sum_a_mats = find_detailed_cell_covar((geographic_entity, tuple((geounits_in_geographic_entity, tpd) for tpd in row[1])),
                                                                                     widths_dict, covar_cond_g_minus_block, return_sum_a_mats=True)
    two_pass_dict_parent = tuple(tpd for tpd in row[1] if tpd['geocode'] == parent)[0]

    two_pass_dict_parent["covar_cond_g_minus"] = sum_a_mats.dot(two_pass_dict_parent["covar_cond_g_minus"])
    two_pass_dict_parent["covar_tilde"] = covar
    two_pass_dict_parent["beta_tilde"] = entity_estimate
    two_pass_dict_parent["a_mat"] = sum_a_mats.dot(two_pass_dict_parent["a_mat"])
    return parent, two_pass_dict_parent


def compute_cis(row, alphas, query_mats):
    # This function performs many of the operations described in Algorithm 3 in the companion paper, which can be found at https://arxiv.org/abs/2404.13164; however, the covariance matrix for
    # the detailed cell counts for the geographic entity are computed before this function is called

    geographic_entity, entity_est, covar = row
    entity_query_ests = [query_mat.dot(entity_est) for query_mat in query_mats.values()]
    # Note that the resulting CIs should be interpreted as a set of distinct univariate CIs, since we ignore the covariance between query
    # answers of distinct rows of mat_queries:
    entity_query_std_devs = [np.sqrt(query_mat.dot(covar).dot(query_mat.T)) for query_mat in query_mats.values()]
    res = []
    queries = list(query_mats.keys())
    # Format of each element of res is (geographic_entity, query, alpha, entity_query_est, entity_query_std_dev, MOE)
    for alpha in alphas:
        quantile = gaussian.ppf(1 - alpha / 2)
        for query, entity_query_est, entity_query_std_dev in zip(queries, entity_query_ests, entity_query_std_devs):
            res.append((geographic_entity, query, alpha, float(entity_query_est), float(entity_query_std_dev), float(entity_query_std_dev * quantile)))
    return res


def combine_joined_ests(est_and_var1, est_and_var2, mapping_query1, mapping_query2, marginalize_query1, marginalize_query2, schema_name1, schema_name2):
    # Suppose betaHat2 is a normally distributed random vector with mean beta2 and variance V2, betaHat1 is a normally distributed random vector with mean beta1 and variance V1,
    # that Q1 beta1 = Q2 beta2. Then the estimate of beta1 defined as, betaTilde =
    # arg min_{beta} (betaHat1 - beta)^T V1^{-1} (betaHat1 - beta) + (Q2 betaHat2 - Q1 beta)^T (Q2 V2 Q2^T)^{-1} (Q2 betaHat2 - Q1 beta)
    # is given by betaTilde = C^{-1} d, where C = V1^{-1} + Q1^T S Q1, d = V1^{-1} betaHat1 +  Q1^T S Q2 betaHat2, S = (Q2 V2 Q2^T)^{-1}. Also, the variance of betaTilde is C^{-1}.
    # This function returns both betaTilde and its variance matrix, C^{-1}. First we will define the matrices in these definitions, using this same notation:

    Q1_Q2 = []
    for schema_name, mapping_query, marginalize_query in zip([schema_name1, schema_name2], [mapping_query1, mapping_query2], [marginalize_query1, marginalize_query2]):
        # See the explanation in init_bottom_up_pass() from main_two_pass_alg.py for defining the 'strat' for an explanation of the method below used to define Q1 and Q2:
        schema = SchemaMaker.fromName(schema_name)
        marg_query_facs = [ss.csr_matrix(fac) for fac in schema.getQuery(marginalize_query).kronFactors()]
        mapping_query_facs = [ss.csr_matrix(fac) for fac in schema.getQuery(mapping_query).kronFactors()]
        # applyFunctionToRows([[x[0], x[1], ...]], [[y[0], y[1], ...]], combineRows)[0] provides the Kronecker factors, say [z[0], z[1], ...], of a marginal query group
        # matrix such that, for each z[i], the nonzero elements for columns A and B are in the same row if they are in the same row for either x[i] or y[i]:
        combined_facs = applyFunctionToRows([mapping_query_facs], [marg_query_facs], combineRows)[0]
        combined_row_space_basis = ss.csr_matrix(reduce(ss.kron, combined_facs))
        combined_row_space_basis.eliminate_zeros()
        Q = combined_row_space_basis.dot(schema.getQuery(marginalize_query).matrixRep().T).tocsr()
        Q.eliminate_zeros()
        Q.data = np.ones(Q.data.size)
        Q1_Q2.append(Q.toarray())

    S = np.linalg.inv(Q1_Q2[1].dot(est_and_var2[2]).dot(Q1_Q2[1].T))
    inv_V1 = np.linalg.inv(est_and_var1[2])
    C = inv_V1 + Q1_Q2[0].T.dot(S).dot(Q1_Q2[0])
    d = inv_V1.dot(est_and_var1[1]) + Q1_Q2[0].T.dot(S).dot(Q1_Q2[1]).dot(est_and_var2[1])
    var_betaTilde = np.linalg.inv(C)
    betaTilde = var_betaTilde.dot(d)
    return est_and_var1[0], (est_and_var1[0], betaTilde, var_betaTilde)


def recompute_covar_tilde(block_child_dict, parent_dict, covar_cond_g_minus_block):
    # The fact that each sibling has the same value for Var(\hat{\beta}(c | c-)) allows us to simplify Var(\tilde{\beta}(c)) as:
    block_child_dict["covar_tilde"] = covar_cond_g_minus_block.value * (1 - 1 / block_child_dict["num_siblings"]) + parent_dict["covar_tilde"] / block_child_dict["num_siblings"] ** 2
    return block_child_dict["geocode"], block_child_dict


def find_all_detailed_cell_covar(geocode_rdds, geolevel, widths_dict, reader_path, reader_suffix, covar_cond_g_minus_block, shuffle_partitions_dict, checkpoint_path):
    if geolevel in shuffle_partitions_dict.keys():
        partitions = shuffle_partitions_dict[geolevel]
    else:
        assert geolevel == "Tract" and "Tract_Subset" in shuffle_partitions_dict.keys()
        partitions = shuffle_partitions_dict["Tract_Subset"]

    geounits_in_shortest_paths_df = geocode_rdds[geolevel].flatMap(lambda row: find_geounits_on_shortest_paths(row, widths_dict)).toDF()
    # Row format is Row(geocode: str, geographic_entity: str, geounits_in_geographic_entity: list(str))

    # Joining geounits_in_shortest_paths_df with the output of main_two_pass_alg.py when both are spark data frames is much faster than joining the RDD counterparts of these objects instead, since the later requires creating
    # all the python objects stored in each row of the RDDs computed/saved in main_two_pass_alg.py, including for rows(/geounits) that are not required to estimate the geolevel CIs. The inclusion of the last two arguments of
    # read_parquet_gls_two_pass_node(.) below result in this join being carried out using data frames:
    two_pass_rdd = read_parquet_gls_two_pass_node([reader_path + geolevel + reader_suffix for geolevel in widths_dict.values()], geounits_in_shortest_paths_df, "geocode", partitions)

    # Since we did not save the matrix "covar_tilde" for block geounits, recompute this matrix for each required block geounit:
    widths = sorted(widths_dict.keys())
    block_width = widths[-1]
    block_geocodes = geocode_rdds[geolevel].flatMap(lambda row: list(row[1])).filter(lambda row: len(row) == block_width).collect()

    if len(block_geocodes) > 0:
        bg_width = widths[-2]
        two_pass_rdd_blocks = two_pass_rdd.filter(lambda row: len(row[0]) == block_width and row[0] in block_geocodes)
        two_pass_rdd_non_blocks = two_pass_rdd.filter(lambda row: len(row[0]) != block_width)
        two_pass_rdd_blocks = two_pass_rdd_blocks.map(lambda row: (row[0][:bg_width], row[1])).join(two_pass_rdd_non_blocks)
        # Row format is (BG geocode, (block_two_pass_dict, BG_two_pass_dict))
        two_pass_rdd = two_pass_rdd_blocks.map(lambda row: recompute_covar_tilde(row[1][0], row[1][1], covar_cond_g_minus_block)).union(two_pass_rdd_non_blocks)

    two_pass_rdd_subset = two_pass_rdd.map(lambda row: (row[1]["geographic_entity"], (row[1]["geounits_in_geographic_entity"], {k:v for k,v in row[1].items() if k not in ["geographic_entity", "geounits_in_geographic_entity"]})))

    geographic_entity_ests = two_pass_rdd_subset.groupByKey(partitions).map(lambda row: find_detailed_cell_covar(tuple(row), widths_dict, covar_cond_g_minus_block))
    return checkpoint(geographic_entity_ests, None, checkpoint_path + geolevel + "_glsEntityEst.ext", GLSEntityEst, read_parquet_gls_entity_est, repartition=False, partition_size=partitions)


def find_all_detailed_cell_covar_using_recursion(geocode_rdds, geolevel, widths_dict, reader_path, reader_suffix, covar_cond_g_minus_block, shuffle_partitions_dict, checkpoint_path):
    # This function returns the same RDD as find_all_detailed_cell_covar(...), while avoiding the final groupByKey() operation in that function grouping together too many rows in certain cases. The specific case we have in mind
    # is balance-of-counties (ie, subsets within each county outside of census/incorporated places); in this case, using find_all_detailed_cell_covar(...) in this groupByKey() operation creates some rows that are over two
    # gigabytes, which causes a spark error. This function should not be used when it is possible for two geographic entities to be within the same county.

    widths = sorted(widths_dict.keys(), reverse=True)
    paths_dict = {geolev: reader_path + geolev + reader_suffix for geolev in widths_dict.values()}
    widths_rev = list(reversed(widths))
    geounits_in_shortest_paths = geocode_rdds[geolevel].persist()

    def map_to_child_geolevel(row, par_width, child_width):
        geographic_entity, geounit_geocodes = row
        geounit_geocodes = list(geounit_geocodes)
        children = [geo for geo in geounit_geocodes if len(geo) == child_width]
        parents_wo_children = [geo for geo in geounit_geocodes if len(geo) == par_width]
        if len(children) == 0 and len(parents_wo_children) == 0:
            return []
        res = [Row(geocode=geocode, parent=geocode, geographic_entity=geographic_entity) for geocode in parents_wo_children]
        parents = list(set([geo[:par_width] for geo in children]))
        for parent in parents:
            children_inner = [geo for geo in children if geo[:par_width] == parent]
            res.extend([Row(geocode=geocode, parent=parent, geographic_entity=geographic_entity) for geocode in (children_inner + [parent])])
        return res

    for width_ind, width in enumerate(widths[:-1]):
        geolevel_child = widths_dict[width]
        par_width = widths[width_ind + 1]
        par_geolevel = widths_dict[par_width]
        if par_geolevel == "County":
            break
        geounits_in_shortest_paths_df_inner = geounits_in_shortest_paths.flatMap(lambda row: map_to_child_geolevel(row, par_width, width)).toDF().persist()
        # Row format is Row(geocode: str, parent: str, geographic_entity: str)

        if geounits_in_shortest_paths_df_inner.filter(F.col("geocode") != F.col("parent")).count() == 0:
            # There are not any child geounits to group together to replace the parent geounit with, so
            continue

        two_pass_rdd = read_parquet_gls_two_pass_node([paths_dict[gl] for w, gl in widths_dict.items() if gl in paths_dict.keys() and w in [width, par_width]], geounits_in_shortest_paths_df_inner, "geocode", shuffle_partitions_dict[geolevel_child])
        if geolevel_child == "Block":
            block_geocodes = geounits_in_shortest_paths.flatMap(lambda row: list(row[1])).filter(lambda row: len(row) == width).collect()
            if len(block_geocodes) > 0:
                bg_width = widths[1]
                two_pass_rdd_blocks = two_pass_rdd.filter(lambda row: len(row[0]) == width and row[0] in block_geocodes)
                two_pass_rdd_non_blocks = two_pass_rdd.filter(lambda row: len(row[0]) != width)
                two_pass_rdd_blocks = two_pass_rdd_blocks.map(lambda row: (row[0][:bg_width], row[1])).join(two_pass_rdd_non_blocks)
                # Row format is (BG geocode, (block_two_pass_dict, BG_two_pass_dict))
                two_pass_rdd = two_pass_rdd_blocks.map(lambda row: recompute_covar_tilde(row[1][0], row[1][1], covar_cond_g_minus_block)).union(two_pass_rdd_non_blocks)

        two_pass_rdd_subset = two_pass_rdd.map(lambda row: ((row[1]["geographic_entity"], row[1]["parent"]), {k:v for k,v in row[1].items() if k not in ["geographic_entity", "parent"]}))
        geographic_entity_ests = (two_pass_rdd_subset.groupByKey(shuffle_partitions_dict[geolevel_child])
                                  .map(lambda row: find_detailed_cell_covar_bottom_up(tuple(row), widths_dict, covar_cond_g_minus_block)))
        # Row format is: (parent, new_two_pass_dict_parent)

        new_path = checkpoint_path + par_geolevel + "_" + geolevel + "_glsTwoPassNode.ext"
        write_parquet(geographic_entity_ests, new_path, GLSTwoPassNode)
        paths_dict[par_geolevel] = new_path
        paths_dict.pop(geolevel_child)
        # Remove child geounits from second element of rows in geounits_in_shortest_paths:
        geounits_in_shortest_paths = geounits_in_shortest_paths.map(lambda row: (row[0], list(set([row1i[:par_width] for row1i in row[1]])))).persist()

    # The rest of this function is very similar to find_all_detailed_cell_covar(...)
    partitions = shuffle_partitions_dict[geolevel]
    geounits_in_shortest_paths_df = geounits_in_shortest_paths.flatMap(lambda row: find_geounits_on_shortest_paths(row, widths_dict)).toDF()
    # Row format is Row(geocode: str, geographic_entity: str, geounits_in_geographic_entity: list(str))

    two_pass_rdd = read_parquet_gls_two_pass_node(list(paths_dict.values()), geounits_in_shortest_paths_df, "geocode", partitions)
    two_pass_rdd_subset = two_pass_rdd.map(lambda row: (row[1]["geographic_entity"], (row[1]["geounits_in_geographic_entity"], {k:v for k,v in row[1].items() if k not in ["geographic_entity", "geounits_in_geographic_entity"]})))
    geographic_entity_ests = two_pass_rdd_subset.groupByKey(partitions).map(lambda row: find_detailed_cell_covar(tuple(row), widths_dict, covar_cond_g_minus_block))
    return checkpoint(geographic_entity_ests, None, checkpoint_path + geolevel + "_glsEntityEst.ext", GLSEntityEst, read_parquet_gls_entity_est, repartition=False, partition_size=partitions)


def estimate_cis(reader_path, reader_suffix, tabulation_spine_geodict, geographic_entity_geolevels, schema_name, query_mats, alphas, writer_path, nmf_reader_path, constrs_dir_name, nmf_reader_path2,
                 reader_path2, mapping_query1, mapping_query2, marginal_query1, marginal_query2, schema_name2, shuffle_partitions_dict1, shuffle_partitions_dict2):
    widths_dict, geocode_rdds, covar_cond_g_minus_block = setup_inputs(reader_path, tabulation_spine_geodict, geographic_entity_geolevels, nmf_reader_path, constrs_dir_name)

    cols = [nmf_reader_path2, reader_path2, mapping_query1, mapping_query2, marginal_query1, marginal_query2, schema_name2]
    if nmf_reader_path2 is not None:
        assert_fun = lambda inpt: isinstance(inpt, str)
        widths_dict2, geocode_rdds2, covar_cond_g_minus_block2 = setup_inputs(reader_path2, tabulation_spine_geodict, geographic_entity_geolevels, nmf_reader_path2, constrs_dir_name)
    else:
        assert_fun = lambda inpt: inpt is None
    for inpt in cols:
        assert assert_fun(inpt)

    for geolevel in geographic_entity_geolevels:
        ests_and_covars = find_all_detailed_cell_covar(geocode_rdds, geolevel, widths_dict, reader_path, reader_suffix, covar_cond_g_minus_block, shuffle_partitions_dict1, writer_path + "checkpoint/est_cis_GLSEntityEst_nmfs1/")
        # Row format is (geographic_entity, entity_estimate, entity_estimate_covar)

        if nmf_reader_path2 is not None:
            ests_and_covars2 = find_all_detailed_cell_covar(geocode_rdds2, geolevel, widths_dict2, reader_path2, reader_suffix, covar_cond_g_minus_block2, shuffle_partitions_dict2, writer_path + "checkpoint/est_cis_GLSEntityEst_nmfs2/")
            ests_and_covars = ests_and_covars.join(ests_and_covars2).map(lambda row: combine_joined_ests(row[1][0], row[1][1], mapping_query1, mapping_query2, marginal_query1, marginal_query2, schema_name, schema_name2))

        ci_rdd = ests_and_covars.flatMap(lambda row: compute_cis(row[1], alphas, query_mats))
        # Row format is (geographic_entity, query, alpha, entity_query_est, entity_query_std_dev, moe)

        write_parquet(ci_rdd, writer_path + "cis/" + geolevel + "_cis.ext", EntityAndQueryCI, save_as_csv=True)
        str_to_print = '\n'.join([str(('geocode', 'query', 'alpha', 'estimate', 'std_dev', 'moe'))] + [str(x) for x in ci_rdd.take(50)])
        print(f"\n\n\n For geolevel {geolevel}, ci_rdd.take(50) = \n{str_to_print}\n\n\n")
