import heapq

from typing import Callable, Generator, Sequence


class SampleTreeNode:
    def __init__(self, name: str, parent: 'SampleTreeNode' = None):
        self.parent = parent
        self.samples = 0
        self.name = name
        self.sum_samples = 0
        self.children = {}

    def __lt__(self, other: 'SampleTreeNode') -> bool:
        return self.samples < other.samples

    def insert(self, name: str, samples: int) -> None:
        self._insert(name.split("/"), samples)

    def _insert(self, components: Sequence[str], samples: int) -> None:
        if not components:
            self.samples += samples
        else:
            self.children.setdefault(components[0], SampleTreeNode(components[0], self))
            self.children[components[0]]._insert(components[1:], samples)
        self.sum_samples += samples

    def leaves(self) -> Generator['SampleTreeNode', 'SampleTreeNode', None]:
        if not self.children:
            yield self
        for child in self.children.values():
            for result in child.leaves():
                yield result

    def _merge_up(self) -> None:
        if not self.parent:
            return self
        self.parent.samples += self.samples
        del self.parent.children[self.name]
        return self.parent

    def prune_until(self, max_leaves: int = 10, min_samples: int = 5) -> None:
        leaves = []
        for leaf in self.leaves():
            leaves.append((leaf.samples, leaf))

        heapq.heapify(leaves)

        while leaves[0][1].parent:
            lowest = heapq.heappop(leaves)
            if lowest[0] > min_samples and len(leaves) < max_leaves:
                break
            new_node = lowest[1]._merge_up()
            if len(new_node.children) == 0:
                heapq.heappush(leaves, (new_node.samples, new_node))

    def __str__(
        self,
        indent: str,
        format_samples: Callable[[Sequence[str]], str],
        is_last: bool = True
    ) -> str:
        result = indent + (is_last and "\\---" or "+---") + self.name + ""
        if self.samples:
            result += "(%s)" % (format_samples(self.sum_samples),)

        next_indent = indent + (is_last and "    " or "|   ")
        sorted_children = sorted(self.children.values(), key=attrgetter('name'))
        for child in sorted_children[:-1]:
            result += "\n" + child.__str__(
                next_indent, format_samples, False)
        if sorted_children:
            result += "\n" + sorted_children[-1].__str__(
                next_indent, format_samples, True)

        return result

