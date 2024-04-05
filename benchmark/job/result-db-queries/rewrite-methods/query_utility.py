from typing import DefaultDict
import copy
from collections import defaultdict, deque
from itertools import count
import graphviz as gv
from queue import Queue

class Relation:
    _ids = count(0)

    def __init__(self, name: str, alias: str, attributes: list[str] | None = None, filters: list[str] | None = None,
                 projections: list[str] | None = None):
        self.id = next(self._ids)
        self.name = name
        self.alias = alias
        self.attributes = [] if attributes is None else attributes
        self.filters = [] if filters is None else filters
        self.projections = [] if projections is None else projections

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return (self.id == other.id)

    def __hash__(self):
        return hash(id)

class Join:
    def __init__(self, left_relation: Relation, right_relation: Relation,
                 left_attributes: list[str], right_attributes: list[str]):
        # check validity of join predicate
        assert(len(left_attributes) == len(right_attributes))

        self.left_relation: Relation = left_relation
        self.right_relation: Relation = right_relation
        self.left_attributes = left_attributes
        self.right_attributes = right_attributes

    def __str__(self):
        return f'{self.left_relation.name} ⋈ {self.right_relation.name}'

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return (((self.left_relation == other.left_relation) and (self.right_relation == other.right_relation)) or
               ((self.left_relation == other.right_relation) and (self.right_relation == other.left_relation)))

    def __hash__(self):
        return hash(repr(self))

class JoinGraph:
    def __init__(self, relations: list[Relation] | None = None, joins: list[Join] | None = None):
        # list of relations in the graph (vertices)
        self.relations = [] if relations is None else relations

        # list of joins in the graph (edges)
        self.joins = [] if joins is None else joins

    def __str__(self):
        s =  f"Relations: {', '.join(map(str, self.relations))}\nJoins:\n"
        for J in self.joins:
            s += f'\t{J}\n'
        return s

    def __repr__(self):
        return str(self)

    def num_relations(self):
        return len(self.relations)

    def num_joins(self):
        return len(self.joins)

    def get_relation(self, r_name: str):
        """Returns the `Relation` object """

    def get_join(self, lhs: Relation, rhs: Relation) -> Join | None:
        """ Returns the join between `lhs` and `rhs`. """
        for join in self.joins:
            if (join.left_relation == lhs and join.right_relation == rhs) or\
               (join.left_relation == rhs and join.right_relation == lhs):
                return join
        return None

    def get_joins(self, relations: set[Relation]) -> set[Join]:
        """ Compute the set of joins between `relations`.

        Parameters
        ----------
        - relations: set[Relation]
            The relation for which to compute the corresponding joins.

        Returns
        -------
        A set of joins connecting `relations`.

        """
        joins: set[Join] = set()
        for join in self.joins:
            if join.left_relation in relations and join.right_relation in relations:
                joins.add(join)
        return joins

    def neighbors(self, r: Relation) -> set[Relation]:
        """ Computes a set of neighbors of `r`.

        Parameters
        ----------
        - r: Relation
            The relation for which to compute the neighbors in the join graph.

        Returns
        -------
        A set of relations representing the neighbors of `r`.
        """
        neighbors = set()
        for j in self.joins:
            if (r.name == j.left_relation.name):
                neighbors.add(j.right_relation)
            elif (r.name == j.right_relation.name):
                neighbors.add(j.left_relation)
        return neighbors

    def reachable(self, r: Relation, excluded_relations: set[Relation]) -> set[Relation]:
        """ Computes the set of relations reachable (via joins) from `r`. While traversing the graph, nodes in the
        `excluded_relations` set are not considered.

        Parameters
        ----------
        - r: Relation
            The relation for which to compute the set of reachable nodes in the join graph.
        - excluded_relations: set[Relation]
            A set of relations that are not considered while traversing the join graph.

        Example
        -------
        Consider the following join graph: A -- B -- C -- D
        Assume that we want to compute the reachable set of relations starting at `B` and we exclude the set of nodes
        {C}. Then the reachable set is only {A} (since `D` cannot be reached via `C`).

        Returns
        -------
        A set of relations that are reachable from `r` without considering relations in `excluded_relations`.
        """
        res: set[Relation] = set()

        # initialize queue with our starting relation
        frontier = Queue()
        frontier.put(r)

        while not frontier.empty():
            current_relation = frontier.get()
            res.add(current_relation)
            for rel in self.neighbors(current_relation).difference(excluded_relations):
                frontier.put(rel)
            excluded_relations.add(current_relation)

        return res

    def connected(self, relations: set[Relation]) -> bool:
        if len(relations) == 0:
            return False
        if len(relations) == 1:
            return True
        nodes_to_check = copy.deepcopy(relations)
        start = nodes_to_check.pop()
        reachable_set = self.reachable(start, set(self.relations).difference(nodes_to_check))
        return reachable_set == relations

    def draw(self):
        """Draw the join graph using graphviz."""
        G = gv.Graph(name='join_graph', comment='The Join Graph')

        # Add the Relations as vertices.
        for rel in self.relations:
            G.node(rel.name, f'<<B>{rel.name}</B>>')

        # Add the Joins as edges with the join predicate as label.
        for J in self.joins:
            G.edge(J.left_relation.name, J.right_relation.name,
                   label=f' {J.left_relation.name}.{J.left_attributes}={J.right_relation.name}.{J.right_attributes}')

        return G

def compute_join_keys(outer_relation: Relation, joins: list[Join]) -> tuple[DefaultDict, DefaultDict]:
    """ Computes the outer and inner join keys of `outer_relation` with `joins`. The `outer_relation` must occur in each
    join.

    Parameters
    ----------
    - outer_relation: Relation
        The outer relation.
    - joins: list[Join]
        A list of joins.

    Returns
    -------
    Two defaultdict(list). The first contains a mapping of the outer relation to its join key attributes. The second one
    contains a mapping of all inner_relations to their join key attributes.
    """
    outer_join_keys = defaultdict(list)
    inner_join_keys = defaultdict(list)
    for join in joins:
        if outer_relation == join.left_relation:
            outer_join_keys[outer_relation].extend(join.left_attributes)
            inner_join_keys[join.right_relation].extend(join.right_attributes)
        else:
            outer_join_keys[outer_relation].extend(join.right_attributes)
            inner_join_keys[join.left_relation].extend(join.left_attributes)
    return outer_join_keys, inner_join_keys

def create_adjacency_list(edge_list):
    """
    Converts an edge list to an adjacency list.
    Note: Generated by ChatGPT

    :param edge_list: list of tuples, each representing an edge (node1, node2)
    :return: dict, adjacency list representation of the graph
    """
    adjacency_list = defaultdict(list)
    for edge in edge_list:
        node1 = edge.left_relation
        node2 = edge.right_relation
        adjacency_list[node1].append(node2)
        adjacency_list[node2].append(node1)
    return adjacency_list

def shortest_path(query_graph, start, goal):
    """
    Finds the shortest path in an undirected, unweighted graph using BFS.

    :param graph: dict, adjacency list representing the graph
    :param start: starting node
    :param goal: target node
    :return: list, shortest path from start to goal or None if no path exists
    """
    graph = create_adjacency_list(query_graph.joins)
    # A queue to store the paths to be checked
    queue = deque([[start]])
    # A set to store the visited nodes to prevent revisiting
    visited = set()

    while queue:
        # Get the first path from the queue
        path = queue.popleft()
        # Get the last node from the path
        node = path[-1]

        # Return path if the goal node is reached
        if node == goal:
            return path

        # If the node hasn't been visited yet
        if node not in visited:
            # Mark the node as visited
            visited.add(node)
            # Go through all adjacent nodes
            for adjacent in graph.get(node, []):
                new_path = list(path)
                new_path.append(adjacent)
                queue.append(new_path)

    # Return None if no path is found
    return None
