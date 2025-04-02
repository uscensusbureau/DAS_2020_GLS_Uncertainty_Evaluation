import numpy as np
from copy import deepcopy


def make_spine(n_levels, fanout_generator, per_vertex_strat, inverse_var_generator):
    """ Defines initial spine, schema, and variance of noisy measurements"""
    vertices = [0]  # The global list of current vertex integer codes
    levels_dict = {k: [] for k in range(n_levels)}   # A list of vertex codes in each level
    levels_dict[0] = [0]
    adj_dict = {}  # Adjacency dictionary, format is: {<parent vertex integer code>: <list of child vertex integer codes>}

    cur_id = 1
    for level in range(n_levels - 1):
        for node in levels_dict[level]:
            fanout = fanout_generator()
            adj_dict[node] = list(range(cur_id, cur_id + fanout))
            levels_dict[level + 1].extend(adj_dict[node])
            vertices.extend(adj_dict[node])
            cur_id += fanout

    inv_vars_dict = {k: np.array([inverse_var_generator() for _ in range(per_vertex_strat.shape[0])]) for k in vertices}
    inv_vars = np.concatenate([inv_vars_dict[k] for k in vertices])
    return vertices, adj_dict, levels_dict, inv_vars, inv_vars_dict


def find_num_block_descendents(vertex_id, adj_dict, n_levels, vertex_level):
    vertices = [vertex_id]
    for level in range(n_levels - vertex_level - 1):
        vertices_updated = []
        for vertex in vertices:
            vertices_updated.extend(adj_dict[vertex])
        vertices = vertices_updated
    return len(vertices)


def make_spine_mat(levels_dict, adj_dict, n_levels):
    """"Creates matrix representation of spine"""
    num_blocks = len(levels_dict[n_levels - 1])
    spine_mat_rows = []

    for level in range(n_levels):
        level_vertices = levels_dict[level]
        cur_index = 0
        for vertex in level_vertices:
            num_block_descendents = find_num_block_descendents(vertex, adj_dict, n_levels, level)
            spine_mat_row = np.zeros(num_blocks)
            spine_mat_row[cur_index:cur_index + num_block_descendents] = 1
            spine_mat_rows.append(spine_mat_row)
            cur_index += num_block_descendents
    return np.vstack(spine_mat_rows)


def make_gls_covar(spine_mat, per_vertex_strat, inverse_variances):
    """ makes the full GLS matrix using the naive approach, so this is only feasible on small scale examples"""
    interLevelStrat = np.kron(spine_mat, per_vertex_strat)
    return np.linalg.inv(interLevelStrat.T.dot(np.diag(inverse_variances)).dot(interLevelStrat))


def two_pass_estimation(inv_vars_dict, per_vertex_strat, vertices, adj_dict, levels_dict, n_levels):
    """ Defines variances using two-pass approach"""

    g_cond_gs = {k: np.linalg.inv(per_vertex_strat.T.dot(np.diag(inv_vars_dict[k])).dot(per_vertex_strat)) for k in vertices}

    # Perform bottom-up pass:
    g_cond_g_minus = deepcopy(g_cond_gs)
    child_var_sum_inv = {}
    ac_mats = {}
    for level in range(n_levels - 2, -1, -1):
        parents = levels_dict[level]
        for par in parents:
            child_var_sum_inv[par] = np.linalg.inv(np.sum([g_cond_g_minus[k] for k in adj_dict[par]], axis=0))
            g_cond_g_minus[par] = np.linalg.inv(np.linalg.inv(g_cond_gs[par]) + child_var_sum_inv[par])
            for child in adj_dict[par]:
                ac_mats[child] = g_cond_g_minus[child].dot(child_var_sum_inv[par])

    # Perform top-down pass:
    var_tilde_g = {0: g_cond_g_minus[0]}
    for level in range(n_levels - 1):
        parents = levels_dict[level]
        for par in parents:
            for child in adj_dict[par]:
                var_tilde_g[child] = g_cond_g_minus[child] + ac_mats[child].dot(var_tilde_g[par]).dot(ac_mats[child].T) - ac_mats[child].dot(g_cond_g_minus[child])
    return var_tilde_g, ac_mats, g_cond_g_minus


def find_ancestors(adj_dict, levels_dict, vertices, vertex):
    """ Provides a list of all ancestor vertices of vertex, including vertex itself. Note that this is not the most computationally efficient approach that is possible. """
    ancestors = [vertex]
    for _ in range(len(levels_dict)):
        for vert in vertices:
            if vert in adj_dict.keys() and ancestors[-1] in adj_dict[vert]:
                ancestors.append(vert)
                break
    return ancestors


def find_shortest_path(adj_dict, levels_dict, vertices, vert1, vert2):
    """ Provides the shortest path from vert1 to vert2 in the graph encoded by the adjacency dictionary adj_dict"""
    ancestors1 = find_ancestors(adj_dict, levels_dict, vertices, vert1)
    ancestors2 = find_ancestors(adj_dict, levels_dict, vertices, vert2)

    common_ancestor_ind = np.argmax(np.isin(ancestors1, ancestors2))
    common_ancestor = ancestors1[common_ancestor_ind]
    shortest_path = ancestors1[:common_ancestor_ind + 1]
    shortest_path.extend(reversed([x for x in ancestors2 if x > common_ancestor]))
    return shortest_path, common_ancestor


def compute_covar(var_tilde_g, ac_mats, g_cond_g_minus, adj_dict, levels_dict, n_levels, vertices, vert1, vert2):
    """ Computes cov(\tilde{\beta}(vert1), \tilde{\beta}(vert2))"""
    if vert1 == vert2:
        return var_tilde_g[vert1]
    path, ancestor = find_shortest_path(adj_dict, levels_dict, vertices, vert1, vert2)
    if vert2 == ancestor:
        res = np.eye(ac_mats[path[0]].shape[0])
        for vert in path[:-1]:
            res = res.dot(ac_mats[vert])
        return res.dot(var_tilde_g[vert2])
    if vert1 == ancestor:
        # As is done in the paper, return the transpose of the output of this function after switching vert1 and vert2 inputs in this case:
        return compute_covar(var_tilde_g, ac_mats, g_cond_g_minus, adj_dict, levels_dict, n_levels, vertices, vert2, vert1).T
    ancestor_ind = np.nonzero(np.array(path) == ancestor)[0][0]
    vert1_prime = path[ancestor_ind - 1]
    vert2_prime = path[ancestor_ind + 1]
    sibling_cov = ac_mats[vert1_prime].dot(var_tilde_g[ancestor]).dot(ac_mats[vert2_prime].T) - ac_mats[vert1_prime].dot(g_cond_g_minus[vert2_prime])
    res = np.eye(ac_mats[path[0]].shape[0])
    for vert in path[:ancestor_ind - 1]:
        res = res.dot(ac_mats[vert])
    res = res.dot(sibling_cov)
    for vert in path[ancestor_ind + 2:]:
        res = res.dot(ac_mats[vert].T)
    return res


def main(n_levels, fanout_generator, per_vertex_strat, inverse_var_generator, tol):
    """This function iterates through all pairs of vertices in a random graph, and in each case tests if two covariance matrix estimates
    are the same. The first covariance matrix estimate is defined using the standard approach of computing the GLS estimate covariance
    matrix, which requires evaluating the inverse of a large dense matrix. The second covariance matrix estimate is found using the
    two-pass approach described by Cumings-Menon (2024). An error is returned if any of the elements of these two covariance matrix
    estimates are greater than tol in absolute value."""

    vertices, adj_dict, levels_dict, inv_vars, inv_vars_dict = make_spine(n_levels, fanout_generator, per_vertex_strat, inverse_var_generator)
    spine_mat = make_spine_mat(levels_dict, adj_dict, n_levels)
    covar = make_gls_covar(spine_mat, per_vertex_strat, inv_vars)
    var_tilde_g, ac_mats, g_cond_g_minus = two_pass_estimation(inv_vars_dict, per_vertex_strat, vertices, adj_dict, levels_dict, n_levels)

    print(f"The encoding of the random graph in an adjacency dictionary format is given by: {adj_dict}")
    for vert1 in vertices:
        for vert2 in vertices:
            covar_est = compute_covar(var_tilde_g, ac_mats, g_cond_g_minus, adj_dict, levels_dict, n_levels, vertices, vert1, vert2)
            B1 = np.kron(spine_mat[vert1], np.eye(per_vertex_strat.shape[1]))
            B2 = np.kron(spine_mat[vert2], np.eye(per_vertex_strat.shape[1]))
            covar_actual = B1.dot(covar.dot(B2.T))
            diff_mat = covar_actual - covar_est
            assert np.all(np.abs(diff_mat.flatten()) <= tol), f"For vert1={vert1} and vert2={vert2},\n{covar_actual} \n- \n{covar_est}\n=\n{diff_mat}\nhas element(/s) that are more than {tol} from zero."
    print("All conditions are satisfied.\n")


if __name__ == "__main__":
    #################################
    ## Set user choice parameters: ##
    #################################
    n_levels = 5
    fanout_generator = lambda: np.random.randint(1, 4)  # Used to generate the number of children of each parent vertex
    inverse_var_generator = lambda: np.random.uniform(0, 2)  # Used to generate the reciprocal of the variance of each noisy measurement
    per_vertex_strat = np.array([[1, 1], [1, 0], [0, 1]])  # The rows encode the coefficient matrix for each vertex
    tol = 1e-14  # Numerical tolerance for final validation checks
    #################################

    main(n_levels, fanout_generator, per_vertex_strat, inverse_var_generator, tol)
