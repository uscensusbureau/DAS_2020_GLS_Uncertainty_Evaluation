import sys, os
from functools import reduce
import pytest
import numpy as np
from scipy import sparse as ss

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "das_decennial"))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "dependencies"))

from GLS.das_decennial.programs.optimization.geo_optimizer_decomp import applyFunctionToRows, combineRows, secondCoarsensFirst
from GLS.main_two_pass_alg import make_proj_mat


def test_make_proj_mat() -> None:
    np.set_printoptions(threshold=1000, linewidth=1000)

    ns = [2, 3, 2, 2]  # Schema before marginalizing using marg_query
    query_dims_to_keep = [(0, 1), (), (0, 1, 2, 3), (1, 2), (2, 3), (0, 2)]  # Attribute indices that are kept in each of the query group matrices included in the strategy, eg, () corresponds to the total query
    dims_to_keep = (0, 1)  # Each attribute index not included in dims_to_keep will be summed over in marg_query
    assert 1 in dims_to_keep and ns[1] == 3, "1 must be in dims_to_keep, since we define marg_query_facs so that this query marginalizes over a subset of the levels of this dimension"

    query_mat_facs = []
    for keep_dims in query_dims_to_keep:
        facs = []
        for dim_ind, ni in enumerate(ns):
            if dim_ind in keep_dims:
                facs.append(ss.eye(ni, dtype=np.int8))
            else:
                facs.append(ss.coo_matrix(np.ones((1, ni), dtype=np.int8)))
        query_mat_facs.append(facs)
        print(f"For keep_dims={keep_dims}, facs={[fac.toarray() for fac in facs]}")

    marg_query_facs = []
    for dim_ind, ni in enumerate(ns):
        if dim_ind == 1:
            marg_query_facs.append(ss.coo_matrix(np.array([[1,0,0],[0,1,1]]), dtype=np.int8))
        elif dim_ind in dims_to_keep:
            marg_query_facs.append(ss.eye(ni, dtype=np.int8))
        else:
            marg_query_facs.append(ss.coo_matrix(np.ones((1, ni), dtype=np.int8)))
    print(f"marg_query_facs={[fac.toarray() for fac in marg_query_facs]}")
    mq_mat = reduce(ss.kron, marg_query_facs)

    expected_marginalized_query_answers = [[0.015625, 0.09375, 0.125, 0.75],
                                           [0.015625],
                                           [0.234375, 63.75, 960.0, 261120.0],
                                           [0.046875, 0.9375],
                                           [0.234375],
                                           [0.046875, 0.1875]]
    expected_marginalized_query_group_mats = [[[1, 0, 0, 0], [0, 1, 0, 0], [0, 0, 1, 0], [0, 0, 0, 1]],
                                              [[1, 1, 1, 1]],
                                              [[1, 0, 0, 0], [0, 1, 0, 0], [0, 0, 1, 0], [0, 0, 0, 1]],
                                              [[1, 0, 1, 0], [0, 1, 0, 1]],
                                              [[1, 1, 1, 1]],
                                              [[1, 1, 0, 0], [0, 0, 1, 1]]]
    expected_marginalized_query_answers = [np.array(x) for x in expected_marginalized_query_answers]
    expected_marginalized_query_group_mats = [np.array(x) for x in expected_marginalized_query_group_mats]

    for k, query_facs in enumerate(query_mat_facs):
        query_mat = reduce(ss.kron, query_facs)
        query_mat.eliminate_zeros()
        # applyFunctionToRows([[x[0], x[1], ...]], [[y[0], y[1], ...]], combineRows)[0] provides the Kronecker factors, say [z[0], z[1], ...], of a marginal query group matrix such that, for each z[i], the
        # nonzero elements for columns A and B are in the same row if they are in the same row for either x[i] or y[i]. In other words, combined_row_space_basis below is defined so that its row space is the
        # intersection of the row spaces of query_mat (=reduce(ss.kron, query_facs)) and mq_mat (=reduce(ss.kron, marg_query_facs)):
        combined_facs = applyFunctionToRows([query_facs], [marg_query_facs], combineRows)[0]
        combined_row_space_basis = reduce(ss.kron, combined_facs).tocsr()
        combined_row_space_basis.eliminate_zeros()

        # For the purpose of this test, define the noisy answers so that the sum of each subset of answers is unique:
        noisy_ans = np.array([2 ** (i - 6) for i in range(query_mat.shape[0])])

        # Thus, noisy measurements after marginalizing the query with query matrix given by query_mat to the marginal query group mq_mat are:
        proj_for_query_answer = make_proj_mat(combined_row_space_basis, query_mat)
        answer = proj_for_query_answer.dot(noisy_ans)
        print(f"For query index {k} with facs={[fac.toarray() for fac in facs]}, answer after marginalizing is: {answer}")

        # In the schema with detailed cells corresponding to the levels of the marginal query considered here, the query group matrix of this answer is:
        proj_for_query_answer = make_proj_mat(combined_row_space_basis, mq_mat).toarray()
        print(f"For query index {k}, the query group matrix with domain defined by the histogram after marginalizing is: {proj_for_query_answer}")

        assert np.all(proj_for_query_answer == expected_marginalized_query_group_mats[k])
        assert np.all(answer == expected_marginalized_query_answers[k])
