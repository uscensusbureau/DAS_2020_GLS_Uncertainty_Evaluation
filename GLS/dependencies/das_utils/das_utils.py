# Useful AWS CLI commands
#
# aws s3 ls s3://uscb-decennial-ite-das/
#
# aws s3 rm {path} --recursive --exclude * --include {regex/wildcard/name}
#
# aws s3 cp {from_path} {to_path} --recursive --exclude * --include {regex/wildcard/name} --quiet
#

#####################
# Possibly useful notes:
# https://stackoverflow.com/questions/36994839/i-can-pickle-local-objects-if-i-use-a-derived-class
#####################

import glob
import os
import tempfile
import zipfile
import atexit
import shutil
import numpy as np
from GLS.dependencies.ctools.env import census_getenv
from GLS.dependencies.ctools.paths import mkpath
from GLS.das_decennial.das_constants import CC


DELIM = CC.REGEX_CONFIG_DELIM


def repartitionNodesEvenly(nodes_rdd, num_parts, modified_geoids_dict, is_key_value_pairs=False, partition_size=0):
    """
    Repartitions nodes_rdd so that each partition (other than possibly the final partition) is
    composed of the same number of nodes and so that the nodes in each partition have geocodes
    that would be adjacent to one another in a sorted list of geocodes.
    :param nodes_rdd: RDD of node objects
    :param num_parts: number of partitions of output RDD; only has an impact when part_size==0
    :param modified_geoids_dict: Object (usually spark broadcast variable) whose .value attribute is a dictionary that maps each geoid to a unique integer
    :param is_key_value_pairs: indicates if the nodes_rdd is already in (geoid, node) format
    :param partition_size: the number of nodes in each partition
    """
    # Note that this function assumes modified_geoids_dict.value is a dictionary that maps each geoid to a unique integer
    # in [0, 1, 2, ..., len(modified_geoids_dict.value) - 1].
    assert isinstance(num_parts, int)
    assert isinstance(partition_size, int)

    if num_parts == 0 and partition_size == 0:
        if is_key_value_pairs:
            return nodes_rdd.values()
        else:
            return nodes_rdd

    rdd_count = len(modified_geoids_dict.value)
    # Only use input num_parts when partition_size is not included as input:
    if partition_size == 0:
        partition_size = int(np.ceil(rdd_count / num_parts))

    # Redefine num_parts and partition_size if needed to ensure there are at least min(64, rdd_count) partitions:
    min_partitions = min(64, rdd_count)
    num_parts = max(min_partitions, int(np.ceil(rdd_count / partition_size)))
    if num_parts == min_partitions:
        partition_size = int(np.ceil(rdd_count / num_parts))

    geoid_map_fun = lambda geoid: modified_geoids_dict.value[geoid] // partition_size

    if is_key_value_pairs:
        nodes_rdd = nodes_rdd.map(lambda row: (geoid_map_fun(row[0]), row[1]))
    else:
        nodes_rdd = nodes_rdd.map(lambda node: (geoid_map_fun(node.geocode), node))
    print(f"Repartitioning to {num_parts} partitions, each with size {partition_size}")
    return nodes_rdd.partitionBy(num_parts).values()


def ship_files2spark(spark, allfiles: bool = False, subdirs=('programs', 'das_framework', 'etc'), subdirs2root=(), dasmods=()):
    """
    Zips the files in das_decennial folder and indicated subfolders to have as submodules and ships to Spark
    as zip python file.
    Also can ship as a regular file (for when code looks for non-py files)
    :param subdirs:  Subdirectories to add to the zipfile
    :param allfiles: whether to ship all files (as opposed to only python (.py) files)
    :param spark: SparkSession where to ship the files
    :return:
    """

    print("das.utils.:ship_files2spark(spark=%s, allfiles=%s, subfiles=%s, subdirs2root=%s" %
          (spark, allfiles, subdirs, subdirs2root))

    # for path in list(subdirs) + list(subdirs2root):
    #     assert os.path.isdir(path)

    # Create a temporary directory and register it for deletion at program exit
    tempdir = tempfile.mkdtemp()
    atexit.register(shutil.rmtree, tempdir)

    # das_decennial directory
    ddecdir = os.path.dirname(__file__)
    zipf = zipfile.ZipFile(os.path.join(tempdir, 'das_decennial_submodules.zip'), 'w', zipfile.ZIP_DEFLATED)

    # File with extension to zip
    pat = '*.*' if allfiles else '*.py'

    def addfile(fname: str, arcname: str) -> None:
        # print(f" {fname} -> {arcname}")
        zipf.write(fname, arcname=arcname)

    # Add the files to zip, keeping the directory hierarchy
    # Don't ship /tests/
    for submodule_dir in subdirs:
        files2ship = [fn for fn in glob.iglob(os.path.join(ddecdir, f'{submodule_dir}/**/{pat}'), recursive=True)]
        files2ship = [fn for fn in files2ship if "/tests/" not in fn]

        for fname in files2ship:
            addfile(fname, arcname=fname.split(ddecdir)[1][1:])

    # Add files in the das_decennial directory
    for fullname in glob.glob(os.path.join(ddecdir, pat)):
        zipf.write(fullname, arcname=os.path.basename(fullname))


    # Add files that are imported as if from root
    for subdir2root in subdirs2root:
        for fullname in glob.glob(os.path.join(ddecdir, subdir2root, pat)):
            addfile(fullname, arcname=os.path.basename(fullname))

    # Add modules in /mnt/das_python
    das_pythondir=census_getenv("DAS_PYTHONDIR")
    for dasmod in dasmods:
        modpath=mkpath(das_pythondir,dasmod)
        if os.path.isdir(modpath):
            modfiles = [fn for fn in glob.iglob(os.path.join(das_pythondir, f'{dasmod}/**/{pat}'), recursive=True)]
            # Removed for now, tests shouldn't be very big
            # modfiles = [fn for fn in modfiles if "/tests/" not in fn]

            for fname in modfiles:
                addfile(fname, arcname=fname.split(das_pythondir)[1][1:])

    zipf.close()
    spark.sparkContext.addPyFile(zipf.filename)

    if allfiles:
        spark.sparkContext.addFile(zipf.filename)
