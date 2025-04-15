import numpy as np
import scipy.sparse as ss
from copy import deepcopy


def findNonzeroCols(constr_mat, row):
    assert type(constr_mat) == ss.csr_matrix
    return constr_mat[row].indices


def applyFunctionToRows(cur_kron_facs, kron_facs, function):
    # This function is designed to take lists, with elements providing the Kronecker factors of each histogram, as inputs rather than query objects.
    cur_kron_facs = deepcopy(cur_kron_facs)
    kron_facs = deepcopy(kron_facs)

    # Both query matrices must have the same number of histogram components:
    assert len(cur_kron_facs) == len(kron_facs), f"{cur_kron_facs}\n\n{kron_facs}"

    # If the current query matrix for histogram i is a StubQuery, replace it with the query matrix of kron_facs:
    cur_kron_facs = [cur_kron_fac if (cur_kron_fac[0] is not None) else deepcopy(kron_fac) for cur_kron_fac, kron_fac in zip(cur_kron_facs, kron_facs)]

    # Both inputs should be a list composed of lists containing None or matrices of class csr_matrix:
    cur_kron_facs = [cur_kron_fac if (cur_kron_fac[0] is None) else [ss.csr_matrix(x) for x in cur_kron_fac] for cur_kron_fac in cur_kron_facs]
    kron_facs = [kron_fac if (kron_fac[0] is None) else [ss.csr_matrix(x) for x in kron_fac] for kron_fac in kron_facs]

    for ihist, (kron_facs_histi, cur_kron_facs_histi) in enumerate(zip(kron_facs, cur_kron_facs)):
        for kdim, (k_kron_fac_histi, k_cur_kron_fac_histi) in enumerate(zip(kron_facs_histi, cur_kron_facs_histi)):
            if k_kron_fac_histi is None:
                continue
            # Convert Kronecker factor into a list containing one list per row indicating the indices of the columns that are nonzero in the row:
            cols_in_each_row1 = [findNonzeroCols(k_cur_kron_fac_histi, row) for row in range(k_cur_kron_fac_histi.shape[0])]
            cols_in_each_row2 = [findNonzeroCols(k_kron_fac_histi, row) for row in range(k_kron_fac_histi.shape[0])]
            # Call user-supplied function and ensure no rows are empty:
            cols_in_each_row1 = [col_inds for col_inds in function(deepcopy(cols_in_each_row1), deepcopy(cols_in_each_row2)) if len(col_inds) > 0]
            # Convert Kronecker factor back to a csr matrix:
            n_elms_in_rows = [len(col_inds1) for col_inds1 in cols_in_each_row1]
            indptr = np.cumsum([0] + n_elms_in_rows)
            indices = np.concatenate(cols_in_each_row1)
            cur_kron_facs[ihist][kdim] = ss.csr_matrix((np.repeat(True, len(indices)), indices, indptr), shape=(len(cols_in_each_row1), k_cur_kron_fac_histi.shape[1]))
    return cur_kron_facs


def combineRows(cols_in_each_row1, cols_in_each_row2):
    for col_inds2 in cols_in_each_row2:
        inds_not_in_a_sub_model = np.setdiff1d(col_inds2, np.concatenate(cols_in_each_row1), assume_unique=True)
        if len(inds_not_in_a_sub_model) > 0:
            cols_in_each_row1.append(inds_not_in_a_sub_model)
        in_sub_model = np.array([np.any(np.isin(sub_model, col_inds2, assume_unique=True)) for sub_model in cols_in_each_row1])
        # Combine sub_models if they both depend on one or more identical variables:
        num_sub_models = np.sum(in_sub_model)
        if num_sub_models > 1:
            to_combine = np.nonzero(in_sub_model)[0]
            cols_in_each_row1[to_combine[0]] = np.concatenate([sub_model for k, sub_model in enumerate(cols_in_each_row1) if k in to_combine])
            cols_in_each_row1 = [sub_model for k, sub_model in enumerate(cols_in_each_row1) if k not in to_combine[1:]]
    return cols_in_each_row1


def secondCoarsensFirst(csrQ1, csrQ2):
    # Checks to see that each row in csrQ2 can be defined as a sum of rows in csrQ1. If this is not the case, this function returns (False, None),
    # and if it is it returns (True, rows_of_first_needed_for_rows_of_second), where rows_of_first_needed_for_rows_of_second[i] is a list of row indices of
    # csrQ1 such that summing these row vectors together derives csrQ2[i, :].

    msg = "secondCoarsensFirst() assumes all nonzero elements of constraint matrices are equal to one."
    assert np.all(np.asarray(csrQ1.data) == 1), msg
    assert np.all(np.asarray(csrQ2.data) == 1), msg
    rows_of_first_needed_for_rows_of_second = []
    for row2_ind in range(csrQ2.shape[0]):
        rows_of_first_needed_for_row2 = []
        # Initialize cols2 as the set of nonzero columns of csrQ2[row2_ind, :]:
        cols2 = findNonzeroCols(csrQ2, row2_ind)
        for row1_ind in range(csrQ1.shape[0]):
            # Check if we can already derive csrQ2[row2_ind, :] using the rows of csrQ1 with indices given by rows_of_first_needed_for_row2:
            if len(cols2) == 0:
                break
            cols1 = findNonzeroCols(csrQ1, row1_ind)
            mask = np.isin(cols1, cols2)
            # Check if the nonzero columns of csrQ1[row1_ind, :] are a subset of cols2:
            if np.all(mask):
                # Remove the nonzero columns of csrQ1[row1_ind, :] from cols2 and add row1_ind to the list of row indices of csrQ1 that are required to derive csrQ2[row2_ind, :]:
                cols2 = np.setdiff1d(cols2, cols1)
                rows_of_first_needed_for_row2.append(row1_ind)
        if len(cols2) > 0:
            return False, None
        rows_of_first_needed_for_rows_of_second.append(rows_of_first_needed_for_row2)
    return True, rows_of_first_needed_for_rows_of_second
