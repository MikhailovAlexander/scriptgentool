from enum import Enum
from typing import Callable


class VertexColor(Enum):
    """Enumerable class for vertex colors used in topological sorting.
    """

    WHITE = 1
    GRAY = 2
    BLACK = 3


class GraphHasLoopError(Exception):
    """A graph with a loop can't be sorted.
    The exception is raised in this case.
    """

    def __init__(self, message="An oriented graph has loop"):
        """
        :param message:  error case description. Default value is
        "An oriented graph has loop".
        """

        self.message = message
        super().__init__(self.message)


class TopoSorter:
    """TopoSorter is used for topological sorting of a directed graph.

    Note: A graph with a loop can't be sorted. TotoSorter raises
    GraphHasLoopError in this case.

    Properties
    ----------
    vertices: tuple[str]
        Tuple of graph vertices names.
    order: int
        Number of graph vertices.
    edges: tuple[tuple[str, str]]
        Tuple of graph edges. Each edge is a tuple containing the names of the
        source and target vertices.
    has_loop: bool
        Shows whether the graph contains a loop.
    topo_sorted_vertices: list[str]
        Sorts the vertices of the graph in topological order. Returns sorted
        list of graph vertices names.

    Methods
    -------
    set_vertices_and_clean_edges(self, vertices: list[str])
        Sets the vertices of the graph from the argument and cleans the
        list of edges.
    has_vertex(self, vertex: str)
        Shows whether the graph contains the vertex from argument.
    add_edge(self, edge: tuple[str, str]):
        Adds edge into the graph edge list.
    add_edges(self, edges: list[tuple[str, str]])
        Adds edges into the graph edges list.
    """

    def __init__(self, vertices: list[str],
                 edges: list[tuple[str, str]] = []):
        """
        :param vertices: list[str]
            List of graph vertex names.
        :param edges: list[tuple[str, str]]
            List of graph edges. Each edge is a tuple containing the names of
            the source and target vertices.
            The default value is an empty list.
        """

        self.__vertices: list[str] = []
        self.__edges: list[tuple[str, str]] = []
        self.set_vertices_and_clean_edges(vertices)
        self.add_edges(edges)

    def __str__(self) -> str:
        return f"vertices:\n{', '.join(self.__vertices)}\n" \
               f"edges:\n{self.__edges_to_str()}"

    def set_vertices_and_clean_edges(self, vertices: list[str]) -> None:
        """Sets the vertices of the graph from the argument and cleans the
        list of edges.

        :param vertices: list of graph vertices names.
        :raise ValueError: when the vertices list contains duplicates.
        :return: None
        """

        if len(vertices) == 0:
            self.__vertices = []
        if len(set(vertices)) != len(vertices):
            raise ValueError("vertices contains duplicates")
        self.__edges = []
        self.__vertices = vertices

    def has_vertex(self, vertex: str) -> bool:
        """Shows whether the graph contains the vertex.

        :param vertex: vertex to search in the graph.
        :return: True if graph contains the vertex otherwise False
        """

        return vertex in self.__vertices

    def add_edge(self, edge: tuple[str, str]) -> None:
        """Adds edge into the graph edges list.

        :param edge: a tuple containing the names of the source and target
        vertices
        :raise ValueError: when the source or target vertex is not in the graph
        :return: None
        """

        for vertex in edge:
            if not self.has_vertex(vertex):
                raise ValueError(f"vertex: {vertex} is not in graph")
        self.__edges.append(edge)

    def add_edges(self, edges: list[tuple[str, str]]) -> None:
        """Adds edges into the graph edges list.

        :param edges: list of graph edges. Each edge is a tuple containing the
        names of the source and target vertices.
        :raise ValueError: when the source or target vertex from some edge
        is not in the graph
        :return: None
        """

        for edge in edges:
            self.add_edge(edge)

    @property
    def vertices(self) -> tuple[str]:
        """
        :return: a tuple of graph vertices names.
        """

        return tuple(self.__vertices)

    @property
    def order(self) -> int:
        """
        :return: a number of graph vertices.
        """

        return len(self.__vertices)

    @property
    def edges(self) -> tuple[tuple[str, str]]:
        """
        :return: tuple of graph edges.
        """
        return tuple(self.__edges)

    @property
    def has_loop(self) -> bool:
        """Shows whether the graph contains a loop.
        :return: True if the graph contains a loop otherwise False.
        """

        colored_vertices = {vertex: VertexColor.WHITE
                            for vertex in self.__vertices}
        adj_dict = self.__get_adg_dict()

        def pass_func(vertex: str):
            pass
        try:
            for vertex in self.__vertices:
                self.__dfs(vertex, colored_vertices, adj_dict, pass_func)
        except GraphHasLoopError as ex:
            return True
        return False

    @property
    def topo_sorted_vertices(self) -> list[str]:
        """Sorts the vertices of the graph in topological order.
        :raises GraphHasLoopError: A graph with a loop can't be sorted.
        TotoSorter raises GraphHasLoopError in this case.
        :return: a sorted list of graph vertices names.
        """

        colored_vertices = {vertex: VertexColor.WHITE
                            for vertex in self.__vertices}
        adj_dict = self.__get_adg_dict()
        processed_vertices = []

        def vertex_process(vertex):
            processed_vertices.append(vertex)
        for vertex in self.__vertices:
            self.__dfs(vertex, colored_vertices, adj_dict, vertex_process)
        return processed_vertices[::-1]

    def __get_adg_dict(self) -> dict[str: list[str]]:
        """
        :return: a dictionary with vertices as a keys and lists of the adjacent
        vertices as a values.
        """

        adj_dict = {vertex: [] for vertex in self.__vertices}
        for edge in self.__edges:
            (adj_dict[edge[0]]).append(edge[1])
        return adj_dict

    def __dfs(self, vertex: str, colored_vertices: dict[str: VertexColor],
              adj_dict: dict[str: list[str]],
              vertex_process: Callable[[str], None]) -> None:
        """A recursive algorithm for depth-first search in the graph.
        Runs the vertex_process function for each vertex of the graph at the
        end of processing.

        :param vertex: a current vertex.
        :param colored_vertices: a dictionary with vertices as a keys
        and vertices colors as a values.
        :param adj_dict: a dictionary with vertices as a keys and lists of the
        adjacent vertices as a values.
        :param vertex_process: a function run at the end of vertex processing.
        :raise: GraphHasLoopError
        :return: None
        """

        if colored_vertices[vertex] == VertexColor.BLACK:
            return
        colored_vertices[vertex] = VertexColor.GRAY
        for adj_vertex in adj_dict[vertex]:
            if colored_vertices[adj_vertex] == VertexColor.WHITE:
                self.__dfs(adj_vertex, colored_vertices, adj_dict,
                           vertex_process)
            if colored_vertices[adj_vertex] == VertexColor.GRAY:
                raise GraphHasLoopError()
        colored_vertices[vertex] = VertexColor.BLACK
        vertex_process(vertex)

    def __edges_to_str(self) -> str:
        """
        :return: a string representation of a list of graph vertices
        """

        return ", ".join([f"{edge[0]}->{edge[1]}" for edge in self.__edges])
