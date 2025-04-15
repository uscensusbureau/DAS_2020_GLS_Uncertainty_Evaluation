import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "das_decennial"))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "dependencies"))

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

from pyspark.sql import SparkSession
from GLS.dependencies.das_utils.das_utils import ship_files2spark
import pyspark.sql.types as T

from GLS.main_two_pass_alg import run_two_pass_estimate
from GLS.est_cis import estimate_cis
from GLS.create_pl94_ci_queries import create_pl94_query_mats
from GLS.das_decennial.das_constants import CC


##########
# User choice parameters:
##########

# Options for run_two_pass_estimate(.):
TPA_WRITER_PATH = "<insert two-pass-algorithm writer path for persons universe Redistricting Data File run>"
NMF_READER_PATH = "<insert path to persons universe Redistricting Data File US NMFs>"

SCHEMA = CC.SCHEMA_PL94

GEOLEVELS = ["US", "State", "County", "Tract", "Block_Group", "Block"]

MARGINALIZE_QUERY = None
# This code does not impose state total population invariant constraints, but instead we can optionally add a fake total population
# noisy measurement to each state geounit (and/or the US geounit) given by the true total population:
TOTAL_INVARIANTS_GEOLEVELS = []
# Do not include "State" in the list above because, for states with an AIAN area, the PL94 state total pop invariants span multiple geounits in the state geolevel (ie, the geounits corresponding to the AIAN and non-AIAN parts of the state). We could include "US", but the US total pop is fairly accurate even without this invariant.

# We call the estimation algorithm two times in this script; the second time uses the DHCP NMFs using the paths below:

TPA_WRITER_PATH_RUN2 = "<insert two-pass-algorithm writer path for DHCP run>"
NMF_READER_PATH_RUN2 = "<insert path to DHCP US NMFs>"
SCHEMA_RUN2 = CC.SCHEMA_DHCP

GEOLEVELS_RUN2 = ["US", "State", "County", "Prim", "Tract_Subset_Group", "Tract_Subset", "Block_Group", "Block"]

MARGINAL_QUERY2_RUN2 = "hhgq8lev * votingage * hispanic * cenrace"
MARGINAL_QUERY1_RUN2 = "detailed"
MAPPING_QUERY1_RUN2 = "detailed"
MAPPING_QUERY2_RUN2 = "hhgq8lev * votingage * hispanic * cenrace"

# Options for est_cis(.):
CI_GEOLEVELS = ["US", "State", "County", "Tract"]

# Select which functions will be called:
CALL_TWO_PASS_ESTIMATE_ALG1 = True
CALL_TWO_PASS_ESTIMATE_ALG2 = True
CALL_ESTIMATE_CIS = True

##########
# The remaining options will generally not need to be changed:
##########
TABULATION_SPINE_GEODICT = {0: "US", 2: "State", 5: "County", 11: "Tract", 12:"Block_Group", 16: "Block"}
PARTITION_SIZE = 4

# The number of non-unique characters that are added to each query name in the NMFs. For example, for the released NMFs, we
# add "_dpq" to the end of these names, so this is 4 in this case:
QUERY_NAME_SUFFIX_LEN = 4
READER_SUFFIX = "_gls.ext"
ALPHAS = [0.05, 0.1]
CONSTRS_DIR_NAME = "Constraint"
DPQUERY_DIR_NAME = "DPQuery"

QUERY_MATS = create_pl94_query_mats()
CI_READER_PATH = TPA_WRITER_PATH + "gls_est/"
CI_READER_PATH_RUN2 = TPA_WRITER_PATH_RUN2 + "gls_est/"

shuffle_partitions_dict_pl94 = {"US": 1, "State": 100, "County": 5000, "Tract": 100000, "Block_Group": 100000, "Block": 100000}
shuffle_partitions_dict_dhc = {"US": 1, "State": 100, "County": 5000, "Prim": 10000, "Tract_Subset_Group": 10000, "Tract_Subset": 100000, "Block_Group": 100000, "Block": 100000}

if __name__ == "__main__":

    spark = SparkSession.builder.getOrCreate()
    spark.udf.registerJavaFunction("bin2da", "gov.census.das.spark.udf.binaryToDoubleArray", T.ArrayType(T.DoubleType()))
    spark.sparkContext.addPyFile("GLS.zip")

    if CALL_TWO_PASS_ESTIMATE_ALG1:
        run_two_pass_estimate(reader_path=NMF_READER_PATH, geolevels=GEOLEVELS, constrs_dir_name=CONSTRS_DIR_NAME, dpquery_dir_name=DPQUERY_DIR_NAME,
                              partition_size=PARTITION_SIZE, query_name_suffix_len=QUERY_NAME_SUFFIX_LEN, schema_name=SCHEMA, writer_path=TPA_WRITER_PATH,
                              total_invariants_geolevels=TOTAL_INVARIANTS_GEOLEVELS, marginalize_query=MARGINALIZE_QUERY, pl94_reader_path=None,
                              shuffle_partitions_dict=shuffle_partitions_dict_pl94)

    if CALL_TWO_PASS_ESTIMATE_ALG2:
        run_two_pass_estimate(reader_path=NMF_READER_PATH_RUN2, geolevels=GEOLEVELS_RUN2, constrs_dir_name=CONSTRS_DIR_NAME, dpquery_dir_name=DPQUERY_DIR_NAME,
                              partition_size=PARTITION_SIZE, query_name_suffix_len=QUERY_NAME_SUFFIX_LEN, schema_name=SCHEMA_RUN2, writer_path=TPA_WRITER_PATH_RUN2,
                              total_invariants_geolevels=TOTAL_INVARIANTS_GEOLEVELS, marginalize_query=MARGINAL_QUERY2_RUN2, pl94_reader_path=NMF_READER_PATH,
                              shuffle_partitions_dict=shuffle_partitions_dict_dhc)

    if CALL_ESTIMATE_CIS:
        estimate_cis(reader_path=CI_READER_PATH, reader_suffix=READER_SUFFIX, tabulation_spine_geodict=TABULATION_SPINE_GEODICT, geographic_entity_geolevels=CI_GEOLEVELS,
                     schema_name=SCHEMA, query_mats=QUERY_MATS, alphas=ALPHAS, writer_path=TPA_WRITER_PATH, nmf_reader_path=NMF_READER_PATH, constrs_dir_name=CONSTRS_DIR_NAME,
                     nmf_reader_path2=NMF_READER_PATH_RUN2, reader_path2=CI_READER_PATH_RUN2, mapping_query1=MAPPING_QUERY1_RUN2, mapping_query2=MAPPING_QUERY2_RUN2,
                     marginal_query1=MARGINAL_QUERY1_RUN2, marginal_query2=MARGINAL_QUERY2_RUN2, schema_name2=SCHEMA_RUN2, shuffle_partitions_dict1=shuffle_partitions_dict_pl94,
                     shuffle_partitions_dict2=shuffle_partitions_dict_dhc)

    exit(0)
