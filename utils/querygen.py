#!env python3

import os

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2' # silence TnesorFlow INFO and WARNING messages

import argparse
import easygraph as eg
import enum
import functools
import graphviz
import io
import itertools
import math
import numpy
import platform
import random
import scipy
import subprocess
import sys
import tempfile
import warnings


DATABASE = 'db'

class SkewMethod(enum.Enum):
    SELECT_EDGE = 1
    SELECT_SOURCE_THEN_SINK = 2
SKEW_METHOD = SkewMethod.SELECT_EDGE


#=======================================================================================================================
# Helper functions
#=======================================================================================================================

def powerset(iterable):
    s = list(iterable)
    return itertools.chain.from_iterable(itertools.combinations(s, r) for r in range(len(s)+1))

def max_edges(num_nodes :int):
    return num_nodes * (num_nodes - 1) // 2

def default_open(filepath):
    if platform.system() == 'Darwin':       # macOS
        subprocess.call(('open', filepath))
    elif platform.system() == 'Windows':    # Windows
        os.startfile(filepath)
    else:                                   # linux variants
        subprocess.call(('xdg-open', filepath))

def generate_uniform_degrees(G :eg.Graph):
    degrees = list(G.degree().values())
    mean = sum(degrees) // len(G.nodes)
    uniform_degrees = [mean] * len(G.nodes)
    for i in range(sum(degrees) - mean * len(G.nodes)):
        uniform_degrees[i] = uniform_degrees[i] + 1
    return uniform_degrees

def compute_graph_density(G :eg.Graph):
    n = G.number_of_nodes()
    m = G.number_of_edges()
    return 2 * m / (n * (n-1))

def compute_edge_skewness(G :eg.Graph):
    degrees = sorted(list(G.degree().values()))
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('error') # turn warnings into errors
            return abs(scipy.stats.skew(degrees, nan_policy='raise')) # abs(): we don't care whether tail is left or right
    except RuntimeWarning:
        return 0 # on RuntimeWarning, the degrees are identical ⇒ return skew 0

def compute_edge_p_value(G :eg.Graph):
    degrees = list(G.degree().values())
    uniform_degrees = generate_uniform_degrees(G)
    _, p = scipy.stats.chisquare(f_obs=degrees, f_exp=uniform_degrees)
    return p

def compute_edge_stddev(G :eg.Graph):
    degrees = list(G.degree().values())
    return scipy.stats.tstd(degrees)

def compute_edge_entropy(G :eg.Graph):
    degrees = list(G.degree().values())
    uniform_degrees = generate_uniform_degrees(G)
    return scipy.stats.entropy(pk=degrees, qk=uniform_degrees)

def compute_bridges(G :eg.Graph) -> set:
    visited = set()
    bridges = set((e[0], e[1]) for e in G.edges)
    assert eg.is_connected(G), 'graph must be connected'

    def dfs(n :int, path :list=list()):
        visited.add(n)
        neighbors = set(G.neighbors(n))

        #===== Neighbors in the path (excluding the parent) form back edges and back edges form cycles. =====
        assert neighbors.intersection(visited).issubset(path), 'there can only be back edges to nodes in the path'
        back_edges = neighbors.intersection(path)
        if path:
            back_edges.remove(path[-1]) # exclude parent from back edges

        #===== The back edge to the smallest vertex (w.r.t. DFS numbering) on the path is the largest cycle. =====
        if back_edges:
            idx = next(idx for idx, v in enumerate(path) if v in back_edges) # first vertex in path that is reached by a back edge
            cycle = path[idx:]
            cycle.append(n)
            for i in range(0, len(cycle) - 1):
                bridges.discard((cycle[i], cycle[i+1]))
            bridges.discard((cycle[-1], cycle[0]))

        #===== Recurse =====
        path.append(n)
        for s in neighbors:
            if s not in visited:
                dfs(s, path)
        path.pop()

    dfs(0)
    return bridges

def create_bounded_Zipf_distribution(values, seed=None) -> scipy.stats.rv_discrete:
        indices = numpy.arange(1, len(values) + 1, dtype=float) # [1, ..., N+1]
        assert len(indices) == len(values)
        weights = indices ** (-args.skew) # Zipf weight
        assert len(weights) == len(values)
        weights /= weights.sum() # normalize
        return scipy.stats.rv_discrete(values=(values, weights), seed=seed)

def show_graph(filename :str, G :eg.Graph):
    if os.fork() == 0:
        with tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8') as dotfile:
            eg.write_dot(G, dotfile.name)
            dotfile.seek(0)
            #  with tempfile.NamedTemporaryFile() as pdf:
            with open(filename, 'wb') as pdf:
                source = dotfile.read()
                pdf.write(
                    graphviz.Source(source, format='pdf', engine='circo').pipe()
                )
                pdf.flush()
                default_open(pdf.name)


#=======================================================================================================================
# Write contents
#=======================================================================================================================

def write_header(file :io.TextIOBase):
    print(f'CREATE DATABASE {DATABASE};\nUSE {DATABASE};', file=file)

def write_schema(file :io.TextIOBase, G :eg.Graph):
    for v in G.nodes:
        print(f'CREATE TABLE R{v} (', file=file)
        print('  id INT(4)', file=file, end='')
        for n in G.neighbors(v):
            if n > v: # avoid symmetric case
                print(f',\n  fid_R{n} INT(4)', file=file, end='')
        print('\n);', file=file)


def write_query(file :io.TextIOBase, G :eg.Graph):
    relations = list(map(lambda v: f'R{v}', G.nodes)) # generate relation names

    print('SELECT ', file=file, end='')
    print(', '.join(map(lambda R: f'{R}.id', relations)), file=file)

    print('FROM ', file=file, end='')
    print(', '.join(relations), file=file)

    print('WHERE', file=file, end='')
    for i, e in enumerate(G.edges):
        if i != 0: print('  AND', file=file, end='')
        u, v, _ = e
        print(f' {relations[u]}.fid_{relations[v]} = {relations[v]}.id', file=file)

    print(';', file=file)


#=======================================================================================================================
# Generator functions
#=======================================================================================================================

# Creates a graph with \p num_nodes nodes and *no* edges.
def gen_graph(num_nodes :int):
    G = eg.Graph()
    for i in range(num_nodes):
        G.add_node(i, node_attr = { 'name': f'R{i}' })
    return G

def gen_chain(num_nodes :int):
    G = gen_graph(num_nodes)

    for i in range(0, num_nodes - 1):
        G.add_edge(i, i+1)

    return G

def gen_cycle(num_nodes :int):
    G = gen_chain(num_nodes)
    G.add_edge(0, num_nodes - 1)
    return G

def gen_star(num_nodes :int):
    G = gen_graph(num_nodes)

    for i in range(1, num_nodes):
        G.add_edge(0, i)

    return G

def gen_clique(num_nodes :int):
    G = gen_graph(num_nodes)

    for i in range(num_nodes - 1):
        for j in range(i+1, num_nodes):
            G.add_edge(i, j)

    return G

#===== Thinned-out clique ==============================================================================================

def gen_thinned_clique(num_nodes :int, num_thinning :int):
    num_edges = max_edges(num_nodes) - num_thinning
    assert num_edges + 1 >= num_nodes, 'graph would be disconnected'
    G = gen_clique(num_nodes)

    # Create PRNG
    PRNG = numpy.random.Generator(numpy.random.MT19937(seed=args.seed))

    # Remove edges from clique
    for i in range(num_thinning):
        edges = set([(e[0], e[1]) for e in G.edges])
        bridges = set(compute_bridges(G))
        for e in bridges: assert e[0] < e[1]
        for e in edges: assert e[0] < e[1]
        removable_edges = sorted(list(edges - bridges))
        for e in removable_edges: assert e[0] < e[1]
        assert len(removable_edges) > 0, 'there must be another edge that can be removed'

        #===============================================================================================================
        # Select edge to remove next.
        #===============================================================================================================

        if SKEW_METHOD == SkewMethod.SELECT_EDGE:
            # Select edge from all removable edges using a bounded Zipf distribution
            dist = create_bounded_Zipf_distribution(range(len(removable_edges)), seed=PRNG)
            edge_idx = dist.rvs()
            assert edge_idx >= 0 and edge_idx < len(removable_edges)
        elif SKEW_METHOD == SkewMethod.SELECT_SOURCE_THEN_SINK:
            # Select source vertex first, then select removable edge that connects this source.
            # TODO
            raise NotImplementedError()
        else:
            raise ValueError(f'invalid skew method')

        #===== Remove one removable edge. =====
        G.remove_edge(*removable_edges[edge_idx])

    assert eg.is_connected(G)

    return G


#=======================================================================================================================
# main
#=======================================================================================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate problem statements in form of a SQL query and its respective'
                                                 ' schema.')
    parser.add_argument('-q', '--quiet', help='Less output', default=False, action='store_true')
    parser.add_argument('-n', help='Number of relations in the problem', dest='num_relations', type=int, default=3,
                        metavar='N')
    parser.add_argument('-t', help='Type of problem (chain, cycle, star, clique)',
                        dest='query_type', default='chain', action='store', metavar='TYPE')
    parser.add_argument('--count', help='Repeat query multiple times', dest='count', type=int, default=1,
                        metavar='COUNT')
    parser.add_argument('--thinning', help='Number of edges to remove from a clique to create a thinned-out clique '
                                           '(defaults to 0)', dest='num_thinning', type=int,
                        default=0, metavar='K')
    parser.add_argument('--skew', help='Skew factor in Zipf distribution for thinning out cliques', dest='skew',
                        type=float, default=0, metavar='α')
    parser.add_argument('--show', help='Directly show the graph of the generated query', action='store_true')
    parser.add_argument('--seed', help='Seed for the PRNG', type=int, action='store', metavar='SEED', default=None)
    args = parser.parse_args()

    if args.seed:
        if not args.quiet:
            print(f'Seeding PRNG with seed {args.seed}.')
        random.seed(args.seed)
    else:
        random.seed()

    if args.num_thinning != 0:
        assert args.query_type == 'clique', 'thinning is only meaningful for clique queries'
        filename_base = f'{args.query_type}-{args.num_relations}-thinned_{args.num_thinning}'
        G = gen_thinned_clique(args.num_relations, args.num_thinning)
    else:
        filename_base = f'{args.query_type}-{args.num_relations}'
        gen = globals()[f'gen_{args.query_type}']
        G = gen(args.num_relations)

    if args.seed:
        filename_base += f'-seed_{args.seed}'

    filename_schema = filename_base + '.schema.sql'
    filename_query  = filename_base + '.query.sql'

    if args.show:
        show_graph(filename_base + '.pdf', G)

    density = compute_graph_density(G)
    skewness = compute_edge_skewness(G)
    p_value = compute_edge_p_value(G)
    stddev = compute_edge_stddev(G)
    entropy = compute_edge_entropy(G)

    if args.quiet:
        print(f'{filename_schema} {filename_query} {density:.3f} {skewness:.3f} {p_value:.3f} {stddev:.3f} {entropy:.3f}')
    else:
        print(f'Generating problem statement {filename_base}.{{schema,query}}.sql for {args.query_type} query of '
              f'{args.num_relations} relations', end='')
        if args.num_thinning:
            print(f' and thinning out by {args.num_thinning} edges.')
        print(f'  Density: {density:.3f} Skewness: {skewness:.3f} p-value: {p_value:.2f} Stddev: {stddev:.3f} Entropy: {entropy:.3f}')

    with open(filename_schema, 'w') as schema:
        write_header(schema)
        write_schema(schema, G)

    with open(filename_query, 'w') as query:
        write_query(query, G)
