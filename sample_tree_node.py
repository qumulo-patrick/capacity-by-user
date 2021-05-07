class SampleTreeNode:
    def __init__(self, name, parent=None):
        self.parent = parent
        self.samples = 0
        self.name = name
        self.sum_samples = 0
        self.children = {}

    def insert(self, name, samples):
        self.insert_internal(name.split("/"), samples)

    def insert_internal(self, components, samples):
        if not components:
            self.samples += samples
        else:
            self.children.setdefault(components[0], SampleTreeNode(components[0], self))
            self.children[components[0]].insert_internal(components[1:], samples)
        self.sum_samples += samples

    def leaves(self):
        if not self.children:
            yield self
        for child in self.children.values():
            for result in child.leaves():
                yield result

    def merge_up(self):
        if not self.parent:
            return self
        self.parent.samples += self.samples
        del self.parent.children[self.name]
        return self.parent

    def prune_until(self, max_leaves=10, min_samples=5):
        leaves = []
        for leaf in self.leaves():
            leaves.append((leaf.samples, leaf))

        heapq.heapify(leaves)

        while leaves[0][1].parent:
            lowest = heapq.heappop(leaves)
            if lowest[0] > min_samples and len(leaves) < max_leaves:
                break
            new_node = lowest[1].merge_up()
            if len(new_node.children) == 0:
                heapq.heappush(leaves, (new_node.samples, new_node))

    def __str__(self, indent, format_samples, is_last=True):
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

