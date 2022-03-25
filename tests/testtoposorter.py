import unittest
from core.toposorter import TopoSorter, GraphHasLoopError


class TestTopoSorter(unittest.TestCase):
    def setUp(self) -> None:
        self.sorter = TopoSorter([])

    def test__str__empty(self):
        value = "vertices:\n\nedges:\n"
        self.assertEqual(self.sorter.__str__(), value)

    def test__str__single_vertex(self):
        self.sorter.set_vertices_and_clean_edges(["a"])
        value = "vertices:\na\nedges:\n"
        self.assertEqual(self.sorter.__str__(), value)

    def test__str__multi_vertex(self):
        self.sorter.set_vertices_and_clean_edges(["a", "b", "c"])
        value = "vertices:\na, b, c\nedges:\n"
        self.assertEqual(self.sorter.__str__(), value)

    def test__str__multi_vertex_with_edge(self):
        self.sorter.set_vertices_and_clean_edges(["a", "b", "c"])
        self.sorter.add_edge(("a", "b"))
        value = "vertices:\na, b, c\nedges:\na->b"
        self.assertEqual(self.sorter.__str__(), value)

    def test__str__multi_vertex_with_edges(self):
        self.sorter.set_vertices_and_clean_edges(["a", "b", "c"])
        self.sorter.add_edge(("a", "b"))
        self.sorter.add_edge(("b", "c"))
        value = "vertices:\na, b, c\nedges:\na->b, b->c"
        self.assertEqual(self.sorter.__str__(), value)

    def test_set_vertices_and_clean_edges_empty(self):
        self.assertEqual(self.sorter.vertices, tuple())
        self.assertEqual(self.sorter.edges, tuple())

    def test_set_vertices_and_clean_edges_single(self):
        vertices = ["a"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertEqual(self.sorter.vertices, tuple(vertices))
        self.assertEqual(self.sorter.edges, tuple())

    def test_set_vertices_and_clean_edges_multi(self):
        vertices = ["a", "b", "c"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertEqual(self.sorter.vertices, tuple(vertices))
        self.assertEqual(self.sorter.edges, tuple())

    def test_set_vertices_and_clean_duplicate(self):
        vertices = ["a", "b", "b"]
        self.assertRaises(ValueError, self.sorter.set_vertices_and_clean_edges,
                          vertices)

    def test_set_vertices_and_clean_edges_cleaned(self):
        vertices = ["a", "b", "c"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edge(tuple(("a", "b")))
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertEqual(self.sorter.vertices, tuple(vertices))
        self.assertEqual(self.sorter.edges, tuple())

    def test_has_vertex_empty_sorter(self):
        vertex = "a"
        self.assertFalse(self.sorter.has_vertex(vertex))

    def test_has_vertex_single_sorter_true(self):
        vertex = "a"
        vertices = ["a"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertTrue(self.sorter.has_vertex(vertex))

    def test_has_vertex_single_sorter_false(self):
        vertex = "b"
        vertices = ["a"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertFalse(self.sorter.has_vertex(vertex))

    def test_has_vertex_multi_sorter_true(self):
        vertex = "a"
        vertices = ["a", "b", "c"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertTrue(self.sorter.has_vertex(vertex))

    def test_has_vertex_multi_sorter_false(self):
        vertex = "d"
        vertices = ["a", "b", "c"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertFalse(self.sorter.has_vertex(vertex))

    def test_add_edge_success(self):
        vertices = ["a", "b"]
        edge = tuple(("a", "b"))
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edge(edge)
        self.assertEqual(self.sorter.edges, tuple([edge]))

    def test_add_edge_wrong_1_vertex(self):
        vertices = ["a", "b"]
        edge = tuple(["c", "b"])
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertRaises(ValueError, self.sorter.add_edge, edge)

    def test_add_edge_wrong_2_vertex(self):
        vertices = ["a", "b"]
        edge = tuple(["a", "c"])
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertRaises(ValueError, self.sorter.add_edge, edge)

    def test_add_edges_empty(self):
        self.sorter.add_edges([])
        self.assertEqual(self.sorter.edges, tuple())

    def test_add_edges_single(self):
        vertices = ["a", "b"]
        edges = [tuple(("a", "b"))]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edges(edges)
        self.assertEqual(self.sorter.edges, tuple(edges))

    def test_add_edges_multi(self):
        vertices = ["a", "b", "c"]
        edges = [tuple(("a", "b")), tuple(("a", "c"))]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edges(edges)
        self.assertEqual(self.sorter.edges, tuple(edges))

    def test_add_edges_multi_wrong_vertex(self):
        vertices = ["a", "b", "c"]
        edges = [tuple(("a", "b")), tuple(("a", "d"))]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertRaises(ValueError, self.sorter.add_edges, edges)

    def test_vertices_empty(self):
        self.assertEqual(self.sorter.vertices, tuple())

    def test_vertices_single(self):
        vertices = ["a"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertEqual(self.sorter.vertices, tuple(vertices))

    def test_vertices_multi(self):
        vertices = ["a", "b", "c"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertEqual(self.sorter.vertices, tuple(vertices))

    def test_order_empty(self):
        self.assertEqual(self.sorter.order, 0)

    def test_order_single(self):
        vertices = ["a"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertEqual(self.sorter.order, 1)

    def test_order_multi(self):
        vertices = ["a", "b", "c"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertEqual(self.sorter.order, 3)

    def test_edges_empty(self):
        self.assertEqual(self.sorter.edges, tuple())

    def test_edges_single(self):
        vertices = ["a", "b"]
        edges = [tuple(("a", "b"))]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edges(edges)
        self.assertEqual(self.sorter.edges, tuple(edges))

    def test_edges_multi(self):
        vertices = ["a", "b", "c"]
        edges = [tuple(("a", "b")), tuple(("b", "c")), tuple(("c", "a"))]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edges(edges)
        self.assertEqual(self.sorter.edges, tuple(edges))

    def test_has_loop_empty_false(self):
        self.assertFalse(self.sorter.has_loop)

    def test_has_loop_single_false(self):
        vertices = ["a"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertFalse(self.sorter.has_loop)

    def test_has_loop_multi_without_edges_false(self):
        vertices = ["a", "b", "c"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertFalse(self.sorter.has_loop)

    def test_has_loop_multi_with_edges_false(self):
        vertices = ["a", "b", "c"]
        edges = [tuple(("a", "b")), tuple(("b", "c"))]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edges(edges)
        self.assertFalse(self.sorter.has_loop)

    def test_has_loop_multi_with_edges_true(self):
        vertices = ["a", "b", "c"]
        edges = [tuple(("a", "b")), tuple(("b", "c")), tuple(("c", "a"))]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edges(edges)
        self.assertTrue(self.sorter.has_loop)

    def test_has_loop_multi_unconnected_true(self):
        vertices = ["a", "b", "c", "e", "f"]
        edges = [tuple(("a", "b")), tuple(("b", "c")), tuple(("c", "a")),
                 tuple(("e", "f"))]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edges(edges)
        self.assertTrue(self.sorter.has_loop)

    def test_topo_sorted_vertices_empty(self):
        self.assertEqual(self.sorter.topo_sorted_vertices, [])

    def test_topo_sorted_vertices_single(self):
        vertices = ["a"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertEqual(self.sorter.topo_sorted_vertices, vertices)

    def test_topo_sorted_vertices_multi_without_edges(self):
        vertices = ["a", "b", "c"]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.assertCountEqual(self.sorter.topo_sorted_vertices, vertices)

    def __check_toposort(self):
        check_dict = {vertex: number for number, vertex
                      in enumerate(self.sorter.topo_sorted_vertices)}
        for edge in self.sorter.edges:
            source_vertex = edge[0]
            target_vertex = edge[1]
            self.assertGreater(check_dict[target_vertex],
                               check_dict[source_vertex])

    def test_topo_sorted_vertices_multi_with_single_edge(self):
        vertices = ["a", "b", "c"]
        edges = [tuple(("a", "b"))]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edges(edges)
        self.assertCountEqual(self.sorter.topo_sorted_vertices, vertices)
        self.__check_toposort()

    def test_topo_sorted_vertices_multi_with_multi_edges(self):
        vertices = ["a", "b", "c"]
        edges = [tuple(("a", "b")), tuple(("b", "c"))]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edges(edges)
        self.assertCountEqual(self.sorter.topo_sorted_vertices, vertices)
        self.__check_toposort()

    def test_topo_sorted_vertices_reverse_multi_with_multi_edges(self):
        vertices = ["d", "b", "a", "c"]
        edges = [tuple(("a", "b")), tuple(("b", "c")), tuple(("b", "c"))]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edges(edges)
        self.assertCountEqual(self.sorter.topo_sorted_vertices, vertices)
        self.__check_toposort()

    def test_topo_sorted_vertices_with_loop(self):
        vertices = ["d", "b", "a", "c"]
        edges = [tuple(("a", "b")), tuple(("b", "c")), tuple(("c", "a"))]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edges(edges)
        self.assertRaises(GraphHasLoopError, getattr, self.sorter,
                          "topo_sorted_vertices")

    def test_topo_sorted_vertices_over_3_level(self):
        vertices = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
        edges = [tuple(("e", "a")), tuple(("e", "b")), tuple(("b", "j")),
                 tuple(("b", "h")), tuple(("b", "g")), tuple(("h", "d")),
                 tuple(("g", "d")), tuple(("c", "f"))]
        self.sorter.set_vertices_and_clean_edges(vertices)
        self.sorter.add_edges(edges)
        self.assertCountEqual(self.sorter.topo_sorted_vertices, vertices)
        self.__check_toposort()

    def tearDown(self) -> None:
        self.sorter = None


if __name__ == '__main__':
    unittest.main()
