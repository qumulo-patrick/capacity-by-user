#!/usr/bin/env python3

import unittest

from typing import Any, Mapping


from sample_tree_node import SampleTreeNode


class SampleTreeNodeTest(unittest.TestCase):
    def setUp(self):
        self.node = SampleTreeNode('node-name')

    def assertEmpty(self, dictionary: Mapping[Any, Any]) -> None:
        # N.B. empty dictionaries evaluate to false
        self.assertFalse(dictionary)

    def test_default_properties(self) -> None:
        self.assertIsNone(self.node.parent)
        self.assertEqual(self.node.samples, 0)
        self.assertEqual(self.node.name, 'node-name')
        self.assertEqual(self.node.sum_samples, 0)
        self.assertEmpty(self.node.children)

    def test_less_than_is_only_based_off_samples(self) -> None:
        other_node = SampleTreeNode('other-node')
        other_node.samples = 1

        # Should not affect if self.node is less than other_node
        self.node.insert('file', 10)

        self.assertLess(self.node, other_node)

    def test_insert_creates_single_child(self) -> None:
        self.node.insert('file', 5)

        # Original Node should be updated
        self.assertEqual(self.node.sum_samples, 5)
        self.assertEqual(len(self.node.children), 1)
        self.assertIn('file', self.node.children)

        # Child node should have corresponding data
        self.assertEqual(self.node.children['file'].parent, self.node)
        self.assertEqual(self.node.children['file'].name, 'file')
        self.assertEqual(self.node.children['file'].samples, 5)
        self.assertEqual(self.node.children['file'].sum_samples, 5)
        self.assertEmpty(self.node.children['file'].children)

    def test_insert_creates_child_of_child(self) -> None:
        self.node.insert('dir/file', 5)

        # Original Node should be updated
        self.assertEqual(self.node.sum_samples, 5)
        self.assertEqual(len(self.node.children), 1)
        self.assertIn('dir', self.node.children)

        # Child Node should be created
        child_node = self.node.children['dir']
        self.assertEqual(child_node.sum_samples, 5)
        self.assertEqual(len(child_node.children), 1)
        self.assertIn('file', child_node.children)

        # Child of child node should have corresponding data
        self.assertEqual(child_node.children['file'].parent, child_node)
        self.assertEqual(child_node.children['file'].name, 'file')
        self.assertEqual(child_node.children['file'].samples, 5)
        self.assertEqual(child_node.children['file'].sum_samples, 5)
        self.assertEmpty(child_node.children['file'].children)

    def test_insert_accumulates_sum_samples_for_multiple_inserts(self) -> None:
        filenames = ['file1', 'file2', 'file3']
        for filename in filenames:
            self.node.insert(filename, 1)

        self.assertEqual(len(self.node.children), len(filenames))
        self.assertEqual(self.node.sum_samples, len(filenames))

    def test_leaves_of_single_node_only_returns_self(self) -> None:
        leaves = [leaf for leaf in self.node.leaves()]
        self.assertEqual([self.node], leaves)

    def test_leaves_does_not_return_node_if_children_present(self) -> None:
        self.node.insert('file', 5)
        leaves = [leaf for leaf in self.node.leaves()]

        self.assertNotIn(self.node, leaves)
        self.assertEqual(len(leaves), 1)
        self.assertIn(self.node.children['file'], leaves)

    def test_leaves_does_not_return_intermediate_nodes(self) -> None:
        self.node.insert('dir/file', 5)
        leaves = [leaf for leaf in self.node.leaves()]

        self.assertNotIn(self.node.children['dir'], leaves)
        self.assertEqual(len(leaves), 1)
        self.assertIn(self.node.children['dir'].children['file'], leaves)

    def test_leaves_returns_all_leaf_nodes(self) -> None:
        filenames = ['file1', 'file2', 'file3']
        for filename in filenames:
            self.node.insert(filename, 1)

        leaves = [leaf for leaf in self.node.leaves()]
        self.assertEqual(len(leaves), len(filenames))
        for filename in filenames:
            self.assertIn(self.node.children[filename], leaves)

    def test_prune_until_prunes_until_empty(self) -> None:
        self.node.insert('file', 5)
        self.node.prune_until()

        self.assertEmpty(self.node.children)

    def test_prune_stops_if_samples_under_min_and_num_leaves_under_max(
        self
    ) -> None:
        self.node.insert('file', 5)
        self.node.prune_until(max_leaves=2, min_samples=4)

        self.assertEqual(len(self.node.children), 1)
        self.assertIn('file', self.node.children)

    def test_prune_removes_nodes_until_hitting_max_leaves(self) -> None:
        filenames = ['file1', 'file2', 'file3']
        for filename in filenames:
            self.node.insert(filename, 5)

        self.node.prune_until(max_leaves=2, min_samples=4)

        self.assertEqual(len(self.node.children), 2)
        self.assertIn('file1', self.node.children)
        self.assertIn('file2', self.node.children)
        self.assertNotIn('file3', self.node.children)

    def test_prune_removes_nodes_until_hitting_min_samples(self) -> None:
        filenames = ['file1', 'file2', 'file3']
        for filename, samples in zip(filenames, range(3, 6)):
            self.node.insert(filename, samples)

        self.node.prune_until(max_leaves=2, min_samples=4)

        self.assertEqual(len(self.node.children), 1)
        self.assertNotIn('file1', self.node.children)
        self.assertNotIn('file2', self.node.children)
        self.assertIn('file3', self.node.children)


if __name__ == '__main__':
    unittest.main()
