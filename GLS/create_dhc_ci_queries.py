# This file saves the DHC queries that were used in the FPGuide paper to s3; the first two functions are based on code from the file query_tool_stats_dhc.py from the FPGuide repo.
# This script needs to be run before est_cis.py.

import os, sys

from functools import reduce
import numpy as np
from scipy import sparse as ss

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "das_decennial"))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "dependencies"))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from GLS.das_decennial.das_constants import CC
from GLS.das_decennial.programs.schema.schemas.schemamaker import SchemaMaker
from GLS.das_decennial.programs.optimization.geo_optimizer_decomp import secondCoarsensFirst, applyFunctionToRows, combineRows


DHCP_QUERY_PATH = "<insert s3 dhcp query path here>"
DHCH_QUERY_PATH = "<insert s3 dhch query path here>"
DEBUG_MODE = False
MARGINALIZE_QUERY_MAP = {"presence65 * dhch_p19 * hhtenshort_3lev * race * hisp * multig": ["P19", "H4", "PCT14"],
                         "family_nonfamily_size * race * hisp": ["PCT7"],
                         "couple_own_child_rel * race * hisp * sex": ["PCT10"],
                         "is65andover * relgq9cats_hhinst * sex * race7lev * hispanic": ["PCT9"],
                         "hhspchildothgq_hhinst * agepct12 * sex": ["PCT8", "PCT12"],
                         }

tabledict_per = {"PCT8": ["nonvoting", "nonvoting * hhinstlevels", "nonvoting * hhspchildothgq",
                          "childage7lev * hhspchildothgq", "childage7lev * hhinstlevels"],
                 "PCT9": ["65andover", "65andover * hhinstlevels", "65andover * relgq_4_groups",
                          "65andover * relgq8cats * sex", "65andover * relgq8cats"],
                 "PCT9A": ["65andover * whiteAlone", "65andover * hhinstlevels * whiteAlone", "65andover * relgq_4_groups * whiteAlone",
                          "65andover * relgq8cats * sex * whiteAlone", "65andover * relgq8cats * whiteAlone"],
                 "PCT9B": ["65andover * blackAlone", "65andover * hhinstlevels * blackAlone", "65andover * relgq_4_groups * blackAlone",
                          "65andover * relgq8cats * sex * blackAlone", "65andover * relgq8cats * blackAlone"],
                 "PCT9C": ["65andover * aianAlone", "65andover * hhinstlevels * aianAlone", "65andover * relgq_4_groups * aianAlone",
                          "65andover * relgq8cats * sex * aianAlone", "65andover * relgq8cats * aianAlone"],
                 "PCT9D": ["65andover * asianAlone", "65andover * hhinstlevels * asianAlone", "65andover * relgq_4_groups * asianAlone",
                          "65andover * relgq8cats * sex * asianAlone", "65andover * relgq8cats * asianAlone"],
                 "PCT9E": ["65andover * nhopiAlone", "65andover * hhinstlevels * nhopiAlone", "65andover * relgq_4_groups * nhopiAlone",
                          "65andover * relgq8cats * sex * nhopiAlone", "65andover * relgq8cats * nhopiAlone"],
                 "PCT9F": ["65andover * sorAlone", "65andover * hhinstlevels * sorAlone", "65andover * relgq_4_groups * sorAlone",
                          "65andover * relgq8cats * sex * sorAlone", "65andover * relgq8cats * sorAlone"],
                 "PCT9G": ["65andover * tomr", "65andover * hhinstlevels * tomr", "65andover * relgq_4_groups * tomr",
                          "65andover * relgq8cats * sex * tomr", "65andover * relgq8cats * tomr"],
                 "PCT9H": ["65andover * hispTotal", "65andover * hhinstlevels * hispTotal", "65andover * relgq_4_groups * hispTotal",
                          "65andover * relgq8cats * sex * hispTotal", "65andover * relgq8cats * hispTotal"],
                 "PCT9I": ["65andover * notHispTotal * whiteAlone", "65andover * hhinstlevels * notHispTotal * whiteAlone", "65andover * relgq_4_groups * notHispTotal * whiteAlone",
                          "65andover * relgq8cats * sex * notHispTotal * whiteAlone", "65andover * relgq8cats * notHispTotal * whiteAlone"],
                 "PCT12": ["total", "sex", "sex * agepct12"]}

tabledict_unit = {"H4": ["hhtentotal", "hhtenshort_3lev"],
                  "P19": ["hhtentotal", "presence65", "presence65 * size1_size2plus", "presence65 * dhch_pco3_margin3"],
                  "PCT7": ["hhtentotal", "family", "nonfamily", "family_nonfamily_size"],
                  "PCT7A": ["hhtentotal * whitealone", "family * whitealone", "nonfamily * whitealone", "family_nonfamily_size * whitealone"],
                  "PCT7B": ["hhtentotal * blackalone", "family * blackalone", "nonfamily * blackalone", "family_nonfamily_size * blackalone"],
                  "PCT7C": ["hhtentotal * aianalone", "family * aianalone", "nonfamily * aianalone", "family_nonfamily_size * aianalone"],
                  "PCT7D": ["hhtentotal * asianalone", "family * asianalone", "nonfamily * asianalone", "family_nonfamily_size * asianalone"],
                  "PCT7E": ["hhtentotal * nhopialone", "family * nhopialone", "nonfamily * nhopialone", "family_nonfamily_size * nhopialone"],
                  "PCT7F": ["hhtentotal * soralone", "family * soralone", "nonfamily * soralone", "family_nonfamily_size * soralone"],
                  "PCT7G": ["hhtentotal * tomr", "family * tomr", "nonfamily * tomr", "family_nonfamily_size * tomr"],
                  "PCT7H": ["hhtentotal * hispTotal", "family * hispTotal", "nonfamily * hispTotal", "family_nonfamily_size * hispTotal"],
                  "PCT7I": ["hhtentotal * notHispTotal * whitealone", "family * notHispTotal * whitealone",
                            "nonfamily * notHispTotal * whitealone", "family_nonfamily_size * notHispTotal * whitealone"],
                  "PCT10": ["family", "marriedfamily", "dhch_hhtype_p12_part1", "hhtype_dhch_married_fam_pco11_p1", "otherfamily",
                            "sex * otherfamily", "sex * other_with_children_indicator_2", "sex * other_with_children_levels"],
                  "PCT10A": ["family * whitealone", "marriedfamily * whitealone", "dhch_hhtype_p12_part1 * whitealone", "hhtype_dhch_married_fam_pco11_p1 * whitealone", "otherfamily * whitealone",
                            "sex * otherfamily * whitealone", "sex * other_with_children_indicator_2 * whitealone",
                            "sex * other_with_children_levels * whitealone"],
                  "PCT10B": ["family * blackalone", "marriedfamily * blackalone", "dhch_hhtype_p12_part1 * blackalone", "hhtype_dhch_married_fam_pco11_p1 * blackalone", "otherfamily * blackalone",
                            "sex * otherfamily * blackalone", "sex * other_with_children_indicator_2 * blackalone",
                            "sex * other_with_children_levels * blackalone"],
                  "PCT10C": ["family * aianalone", "marriedfamily * aianalone", "dhch_hhtype_p12_part1 * aianalone", "hhtype_dhch_married_fam_pco11_p1 * aianalone", "otherfamily * aianalone",
                            "sex * otherfamily * aianalone", "sex * other_with_children_indicator_2 * aianalone",
                            "sex * other_with_children_levels * aianalone"],
                  "PCT10D": ["family * asianalone","marriedfamily * asianalone", "dhch_hhtype_p12_part1 * asianalone", "hhtype_dhch_married_fam_pco11_p1 * asianalone", "otherfamily * asianalone",
                            "sex * otherfamily * asianalone", "sex * other_with_children_indicator_2 * asianalone",
                            "sex * other_with_children_levels"],
                  "PCT10E": ["family * nhopialone", "marriedfamily * nhopialone", "dhch_hhtype_p12_part1 * nhopialone", "hhtype_dhch_married_fam_pco11_p1 * nhopialone", "otherfamily * nhopialone",
                            "sex * otherfamily * nhopialone", "sex * other_with_children_indicator_2 * nhopialone",
                            "sex * other_with_children_levels * nhopialone"],
                  "PCT10F": ["family * soralone", "marriedfamily * soralone", "dhch_hhtype_p12_part1 * soralone", "hhtype_dhch_married_fam_pco11_p1 * soralone", "otherfamily * soralone",
                            "sex * otherfamily * soralone", "sex * other_with_children_indicator_2 * soralone",
                            "sex * other_with_children_levels * soralone"],
                  "PCT10G": ["family * tomr", "marriedfamily * tomr", "dhch_hhtype_p12_part1 * tomr", "hhtype_dhch_married_fam_pco11_p1 * tomr", "otherfamily * tomr",
                            "sex * otherfamily * tomr", "sex * other_with_children_indicator_2 * tomr",
                            "sex * other_with_children_levels * tomr"],
                  "PCT10H": ["family * hispTotal", "marriedfamily * hispTotal", "dhch_hhtype_p12_part1 * hispTotal", "hhtype_dhch_married_fam_pco11_p1 * hispTotal", "otherfamily * hispTotal",
                            "sex * otherfamily * hispTotal", "sex * other_with_children_indicator_2 * hispTotal",
                            "sex * other_with_children_levels * hispTotal"],
                  "PCT10I": ["family * notHispTotal * whitealone", "marriedfamily * notHispTotal * whitealone", "dhch_hhtype_p12_part1 * notHispTotal * whitealone", "hhtype_dhch_married_fam_pco11_p1 * notHispTotal * whitealone", "otherfamily * notHispTotal * whitealone",
                            "sex * otherfamily * notHispTotal * whitealone", "sex * other_with_children_indicator_2 * notHispTotal * whitealone",
                            "sex * other_with_children_levels * notHispTotal * whitealone"],
                  "PCT14": ["hhtentotal", "multig"],
                  "PCT14A": ["hhtentotal * whitealone", "multig * whitealone"],
                  "PCT14B": ["hhtentotal * blackalone", "multig * blackalone"],
                  "PCT14C": ["hhtentotal * aianalone", "multig * aianalone"],
                  "PCT14D": ["hhtentotal * asianalone", "multig * asianalone"],
                  "PCT14E": ["hhtentotal * nhopialone", "multig * nhopialone"],
                  "PCT14F": ["hhtentotal * soralone", "multig * soralone"],
                  "PCT14G": ["hhtentotal * tomr", "multig * tomr"],
                  "PCT14H": ["hhtentotal * hispTotal", "multig * hispTotal"],
                  "PCT14I": ["hhtentotal * notHispTotal * whitealone", "multig * notHispTotal * whitealone"]}


def create_query_df(tabledict, schema):
    spark = SparkSession.builder.getOrCreate()
    dfs = []
    for tbl in tabledict:
        for qry in tabledict[tbl]:  # Loop through list of each row of the dict
            if qry == "total":  # We add the total query separately in get_queries
                continue
            if DEBUG_MODE == True:
                print("Working on TABLE:", tbl, "QUERY:", qry)
            qry_list = schema.getTableTuples([qry])
            qry_list2 = [(i, qry_list[i]) for i in range(len(qry_list))]  # This adds the index as an element
            qry_df = spark.createDataFrame(qry_list2)
            qry_df_better = (qry_df.withColumnRenamed("_1", "col_index")
                .withColumn("QUERY", F.col("_2._1")).withColumn("LEVEL", F.col("_2._2"))
                .withColumn("TABLE", F.lit(tbl))
                .select("TABLE", "QUERY", "LEVEL", "col_index"))
            dfs.append(qry_df_better)
    return reduce(DataFrame.union, dfs)


def get_queries(prod, tabledict):
    # Creates a list of querynames based on the values in the dictionaries above
    # The list(set()) was added to remove duplicate querynames (for PL, multiple tables use the same queries)
    # The querynames are used later to get the counts from the mdfs
    spark = SparkSession.builder.getOrCreate()
    querynames = sorted(list(set(reduce(lambda x, y: x + y, tabledict.values()))))
    if prod == "DHCP":
        schema = SchemaMaker.fromName(CC.SCHEMA_DHCP)
        queries_df = create_query_df(tabledict, schema)
        # create_query_df doesn't work with total queries, add that row manually
        total_row_p = spark.createDataFrame([("PCT12", "total", "Total", 0)], queries_df.columns)
        queries_df = queries_df.union(total_row_p)
    else:
        schema = SchemaMaker.fromName(CC.SCHEMA_DHCH_TEN_3LEV)
        queries_df = create_query_df(tabledict, schema)
    # drop the cross-tabs we don't use (ex: DHCP doesn't report Householder, spouse, or unmarried partner BY Under 3 years)
    final_query_df_filter = queries_df.withColumn("drop", F.when((F.col("TABLE") == "PCT8") &
                            (F.col("QUERY") == "childage7lev * hhspchildothgq") &
                            (F.substring(F.col("LEVEL"), 1, 11).isin("Householder", "In Group Qu")),
                            F.lit(1)).otherwise(F.lit(0)))
    final_query_df_filter = final_query_df_filter.withColumn("drop", F.when((F.col("TABLE") == "PCT8") &
                           (F.col("QUERY") == "childage7lev * hhinstlevels") &
                           (F.substring(F.col("LEVEL"), 1, 9) == "Household"),
                           F.lit(1)).otherwise(F.col("drop")))
    # note: used substrs below so filters apply to main table and subtables (ex PCT9, PCT9A, ...,  PCT9I)
    final_query_df_filter = final_query_df_filter.withColumn("drop",
                            F.when((F.substring(F.col("TABLE"), 1, 4) == "PCT9") &
                            (F.substring(F.col("QUERY"), 1, 28) == "65andover * relgq8cats * sex") &
                            (F.substring(F.col("LEVEL"), 1, 11) != "Householder"),
                            F.lit(1)).otherwise(F.col("drop")))
    final_query_df_filter = final_query_df_filter.withColumn("drop",
                            F.when((F.substring(F.col("TABLE"), 1, 4) == "PCT9") &
                            (F.substring(F.col("QUERY"), 1, 26) == "65andover * relgq_4_groups") &
                            (F.substring(F.col("LEVEL"), 1, 11) != "Householder"),
                            F.lit(1)).otherwise(F.col("drop")))
    final_query_df_filter = final_query_df_filter.withColumn("drop",
                             F.when((F.substring(F.col("TABLE"), 1, 5) == "PCT10") &
                             (F.substring(F.col("QUERY"), 1, 21) == "dhch_hhtype_p12_part1") &
                             (F.col("col_index") != 0),
                             F.lit(1)).otherwise(F.col("drop")))
    final_query_df_filter = final_query_df_filter.filter("drop != 1").drop("drop")
    # Note that this file is very small, so sorting by row number is not an issue
    final_query_df_filter = final_query_df_filter.withColumn('original_order', F.monotonically_increasing_id())
    final_query_df_filter = final_query_df_filter.withColumn('row_num', F.row_number().over(
        Window.orderBy('original_order'))).drop('original_order')
    if DEBUG_MODE:
        print("final_query_df_filter has", final_query_df_filter.count(), "rows:")
        final_query_df_filter.show(2000, False)
    return final_query_df_filter, querynames


def create_unmarginalized_query_mat(query_data, table_names, schema):
    res = {}
    for row in query_data:
        # Only consider rows in which row["TABLE"] is given by one of the table names, or is given by one of the table names with upper case letters afterward:
        if not np.any([row["TABLE"][:len(table_name)] == table_name and (len(row["TABLE"]) == len(table_name) or row["TABLE"][len(table_name):].isupper()) for table_name in table_names]):
            if DEBUG_MODE:
                print(f"\n create_unmarginalized_query_mat(.) is skipping the following row because its table name is not in {table_names}:\n{row}\n")
            continue
        query_name = row["QUERY"]
        query_mat_index = int(row["col_index"])
        # In the case of DHCH, the TENVACGQ attribute is not in the main histogram, so, at least in this version of the code, ignore queries that are not in the schema:
        try:
            query_mat = schema.getQuery(query_name).matrixRep().tocsr()[query_mat_index]
            query_mat.eliminate_zeros()
        except AssertionError:
            if DEBUG_MODE:
                print(f"\n create_unmarginalized_query_mat(.) is skipping row={row}\n")
            continue
        res[" , ".join([row["QUERY"], row["LEVEL"]])] = query_mat.toarray().flatten()
    return res


def create_marginalized_query_mat(query_data, marginalize_query, table_names, schema):
    mq_facs = [ss.csr_matrix(fac) for fac in schema.getQuery(marginalize_query).kronFactors()]
    mq_mat = ss.csr_matrix(reduce(ss.kron, mq_facs))
    mq_mat.eliminate_zeros()
    res = {}
    for row in query_data:
        # Only consider rows in which row["TABLE"] is given by one of the table names, or is given by one of the table names with upper case letters afterward:
        if not np.any([row["TABLE"][:len(table_name)] == table_name and (len(row["TABLE"]) == len(table_name) or row["TABLE"][len(table_name):].isupper()) for table_name in table_names]):
            if DEBUG_MODE:
                print(f"\n create_marginalized_query_mat(.) is skipping the following row because its table name is not in {table_names}:\n{row}\n")
            continue
        query_name = row["QUERY"]
        query_mat_index = int(row["col_index"])
        # In the case of DHCH, the TENVACGQ attribute is not in the main histogram, so, at least in this version of the code, ignore queries that are not in the schema:
        try:
            query_mat = schema.getQuery(query_name).matrixRep().tocsr()[query_mat_index]
            query_mat.eliminate_zeros()
        except AssertionError:
            if DEBUG_MODE:
                print(f"\n create_marginalized_query_mat(.) is skipping row={row}\n")
            continue

        query_facs = [ss.csr_matrix(fac) for fac in schema.getQuery(query_name).kronFactors()]
        # applyFunctionToRows([[x[0], x[1], ...]], [[y[0], y[1], ...]], combineRows)[0] provides the Kronecker factors, say [z[0], z[1], ...], of a marginal query
        # group matrix such that, for each z[i], the nonzero elements for columns A and B are in the same row if they are in the same row for either x[i] or y[i]:
        combined_facs = applyFunctionToRows([query_facs], [mq_facs], combineRows)[0]
        combined_row_space_full_basis = reduce(ss.kron, combined_facs)

        inner_prod = combined_row_space_full_basis.dot(query_mat.toarray().flatten())
        row_ind = np.nonzero(inner_prod)[0]
        assert len(row_ind) == 1
        combined_row_space_basis = combined_row_space_full_basis.tocsr()[row_ind[0]]
        combined_row_space_basis.eliminate_zeros()

        scf_out = secondCoarsensFirst(mq_mat, query_mat)
        assert scf_out[0], f"The query row is more granular than marginalize_query for row=\n{row}\n\n mq_facs=\n{[mq_fac.toarray() for mq_fac in mq_facs]}\n\nquery_facs=\n{[query_fac.toarray() for query_fac in query_facs]}\n\n scf_out={scf_out}"

        # To project the row space of mq_mat onto combined_row space_basis, we can apply the linear operator x \mapsto combined_row_space_basis.dot(mq_mat.T).dot((mq_mat.dot(mq_mat.T)) ^ {-1}).dot(x).
        # Since this inverse matrix is a diagonal matrix with a diagonal given by one over the row-sums of mq_mat, the following is equivalent and a bit more efficient.
        query_row = ss.csr_matrix(combined_row_space_basis.dot(mq_mat.T))
        query_row.eliminate_zeros()
        # Since each row of combined_row_space_basis is given by the sum over one or more rows of mq_mat, each element of the i^th column of combined_row_space_basis.dot(mq_mat.T) is either the i^th row-sum of mq_mat or zero. So, all nonzero elements
        # of combined_row_space_basis.dot(mq_mat.T).dot((mq_mat.dot(mq_mat.T)) ^ {-1}) are equal to one.
        query_row.data = np.ones(query_row.data.size)
        query_row = ss.csr_matrix(query_row)
        res[" , ".join([row["QUERY"], row["LEVEL"]])] = query_row.toarray().reshape((1, -1))
    return res


def create_dhc_query_mats(schema_name, marginalize_query, marginalize_query_map=MARGINALIZE_QUERY_MAP):
    spark = SparkSession.builder.getOrCreate()
    if schema_name == CC.SCHEMA_DHCP:
        query_data = spark.read.csv(DHCP_QUERY_PATH, sep="|", header=True).collect()
    else:
        assert schema_name == CC.SCHEMA_DHCH_TEN_3LEV
        query_data = spark.read.csv(DHCH_QUERY_PATH, sep="|", header=True).collect()
    query_data = [x.asDict() for x in query_data]
    table_names = marginalize_query_map[marginalize_query]
    schema = SchemaMaker.fromName(schema_name)
    return create_marginalized_query_mat(query_data, marginalize_query, table_names, schema)


def create_unmarginalized_dhc_query_mats(schema_name, table_names):
    spark = SparkSession.builder.getOrCreate()
    if schema_name == CC.SCHEMA_DHCP:
        query_data = spark.read.csv(DHCP_QUERY_PATH, sep="|", header=True).collect()
    else:
        assert schema_name == CC.SCHEMA_DHCH_TEN_3LEV
        query_data = spark.read.csv(DHCH_QUERY_PATH, sep="|", header=True).collect()
    query_data = [x.asDict() for x in query_data]
    schema = SchemaMaker.fromName(schema_name)
    return create_unmarginalized_query_mat(query_data, table_names, schema)


def save_dhc_query_dataframes():
    dhcp_final_query_df_filter, dhcp_querynames = get_queries("DHCP", tabledict_per)
    dhcp_final_query_df_filter.coalesce(1).write.mode("error").csv(DHCP_QUERY_PATH, sep="|", header=True)

    dhch_final_query_df_filter, dhch_querynames = get_queries("DHCH", tabledict_unit)
    dhch_final_query_df_filter.coalesce(1).write.mode("error").csv(DHCH_QUERY_PATH, sep="|", header=True)


if __name__ == "__main__":
    save_dhc_query_dataframes()
    exit(0)
