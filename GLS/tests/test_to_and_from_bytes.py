import sys, os
import pytest
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "das_decennial"))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "dependencies"))

from GLS.two_pass_gls_parquet import from_symmetric_mat_to_bytes, from_bytes_to_symmetric_mat, from_bytes_to_unsymmetric_mat


def test_from_bytes_to_unsymmetric_mat() -> None:
    mats = [np.random.uniform(size=(5,5)), np.random.uniform(size=(50,50)), np.arange(100).reshape((10,10)).astype(np.float64)]
    for mat in mats:
        new_mat = from_bytes_to_unsymmetric_mat(mat.tobytes())
        assert np.all(new_mat == mat)


def test_bytes_to_and_from_symmetric_mat() -> None:
    mats = [np.random.uniform(size=(5,5)), np.random.uniform(size=(50,50)), np.arange(100).reshape((10,10)).astype(np.float64)]
    for mat in mats:
        mat = mat + mat.T
        print(mat)
        new_mat = from_bytes_to_symmetric_mat(from_symmetric_mat_to_bytes({'mat': mat}, 'mat'))
        print(new_mat)
        print()
        assert np.all(new_mat == mat)
