#!/usr/bin/env python3

from qumulo.rest_client import RestClient
import getpass
import os
import pwd
import sys
import ssl
import heapq
from argparse import ArgumentParser
from functools import cmp_to_key
from operator import attrgetter
from multiprocessing import Pool

class SampleTreeNode:
    def __init__(self, name, parent=None):
        self.parent = parent
        self.samples = 0
        self.name = name
        self.sum_samples = 0
        self.children = {}

    def __lt__(self, other):
        if self.parent is None and other.parent is not None:
            return True
        if self.parent is not None and other.parent is None:
            return False
        return (self.parent, self.samples, self.name, self.sum_samples) <  (other.parent, other.samples, other.name, other.sum_samples)

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

def pretty_print_capacity(x):
    start = (1024 ** k for k in (6, 5, 4, 3, 2, 1, 0))
    units = ("E", "P", "T", "G", "M", "K", "b")
    for l, u in zip(start, units):
        if x >= l: return "%0.02f%s" % (x / float(l), u)
    return 0

def get_samples_worker(x):
    credentials, path, n = x
    client = RestClient(credentials["cluster"], credentials["port"])
    client.login(credentials["user"], credentials["password"])
    return client.fs.get_file_samples(path=path, count=int(n), by_value="capacity")

class memoize:
  def __init__(self, function):
    self.function = function
    self.memoized = {}
  def __call__(self, *args):
    try:
      return self.memoized[args]
    except KeyError:
      self.memoized[args] = self.function(*args)
      return self.memoized[args]

def format_owner(cli, auth_id, owner_type, owner_value):
    user = ""
    if owner_type == 'SMB_SID':
        try:
            user_details = cli.ad.sid_to_ad_account(owner_value)
            user = 'AD:' + user_details['name']
        except:
            pass
    elif owner_type == 'NFS_UID':
        try:
            ids = cli.auth.auth_id_to_all_related_identities(auth_id)
        except:
            ids = []
        for i, el in enumerate(ids):
            if el['id_type'] == 'SMB_SID':
                try:
                    user_details = cli.ad.sid_to_ad_account(el['id_value'])
                    if 'group' in user_details['classes']:
                        continue
                    user = 'AD:' + user_details['name']
                except:
                    continue
        if user == "":
            try:
                user = "NFS:%s (id:%s)" % (pwd.getpwuid(int(owner_value)).pw_name, owner_value)
            except:
                pass
    elif owner_type == 'LOCAL_USER':
        user = "LOCAL:%s" % owner_value
    if user == "":
        user = "%s:%s" % (owner_type, owner_value)
    return user

@memoize
def translate_owner_to_owner_string(cli, auth_id, owner_type, owner_value):
    return format_owner(cli, auth_id, owner_type, owner_value)

seen = {}
def get_file_attrs(x):
    credentials, file_ids = x
    client = RestClient(credentials["cluster"], credentials["port"])
    client.login(credentials["user"], credentials["password"])
    result = []
    for file_id in file_ids:
        if file_id in seen:
            result += [seen[file_id]]
            continue
        attrs = client.fs.get_file_attr(file_id)
        str_owner = translate_owner_to_owner_string(client
                                                   , attrs['owner']
                                                   , attrs['owner_details']['id_type']
                                                   , attrs['owner_details']['id_value'])
        seen[file_id] = str_owner
        result.append(str_owner)
    return result

def get_samples(pool, credentials, args):
    return sum(pool.map(
        get_samples_worker,
        ([(credentials, args.path, args.samples / args.concurrency)] * args.concurrency)),
                  [])

def get_owner_vec(pool, credentials, samples, args):
    file_ids = [s["id"] for s in samples]
    sublists = [(credentials, file_ids[i:i+100]) for i in range(0, args.samples, 100)]
    owner_id_sublists = pool.map(get_file_attrs, sublists)
    return sum(owner_id_sublists, [])

def main(args):
    credentials = {"user" : args.user,
                   "password" : args.password,
                   "cluster" : args.cluster,
                   "port" : args.port}

    if credentials["password"] is None:
        credentials["password"] = getpass.getpass(f"Password for {credentials['user']}: ")

    if args.allow_self_signed_server:
        try:
            _create_unverified_https_context = ssl._create_unverified_context
        except AttributeError:
            # Legacy Python that doesn't verify HTTPS certificates by default
            pass
        else:
            # Handle target environment that doesn't support HTTPS verification
            ssl._create_default_https_context = _create_unverified_https_context

    # Qumulo API login
    client = RestClient(args.cluster, args.port)
    client.login(credentials["user"], credentials["password"])

    total_capacity_used = int(
        client.fs.read_dir_aggregates(args.path)['total_capacity'])

    pool = Pool(args.concurrency)

    # First build a vector of all samples...
    samples = get_samples(pool, credentials, args)

    # Then get a corresponding vector of owner strings
    owner_vec = get_owner_vec(pool, credentials, samples, args)

    owners = {}
    directories = {}

    # Create a mapping of user to tree...
    for s, owner in zip(samples, owner_vec):
        owners.setdefault(owner, SampleTreeNode(""))
        owners[owner].insert(s["name"], 1)

    def format_capacity(samples):
        mean = float(samples) / args.samples
        stddev = (((1 - mean) ** 2 * samples +
                   (mean ** 2) * (args.samples - samples)) / args.samples) ** (1/2.)
        confidence =  1.96 * stddev / (args.samples ** (1/2.))

        bytes_per_terabyte = 1000. ** 4
        if args.dollars_per_terabyte != None:
            def to_dollars(adjust): 
                return (
                    (mean + adjust) 
                    * total_capacity_used
                    / bytes_per_terabyte
                    * args.dollars_per_terabyte
                )
            if args.confidence_interval:
                return "[$%0.02f-$%0.02f]/month" % (to_dollars(-confidence),
                                                    to_dollars(confidence))
            else:
                return "$%0.02f/month" % (to_dollars(0),)
        else:
            if args.confidence_interval:
                return "[%s-%s]" % (
                    pretty_print_capacity((mean - confidence) * total_capacity_used),
                    pretty_print_capacity((mean + confidence) * total_capacity_used))
            else:
                return "%s" % pretty_print_capacity((mean) * total_capacity_used)

    print("Total: %s" % (format_capacity(args.samples)))
    sort_fn = lambda x, y: y[1].sum_samples - x[1].sum_samples
    sorted_owners = sorted(owners.items(), key=cmp_to_key(sort_fn))

    # For each owner, print total used, then refine the tree and dump it.
    for name, tree in sorted_owners:
        print("Owner %s (~%0.1f%%/%s)" % (
            name, tree.sum_samples / float(args.samples) * 100,
            format_capacity(tree.sum_samples)))
        if args.fancy_output:  # XXX ezra: This didn't seem very useful to me.
            tree.prune_until(max_leaves=args.max_leaves,
                             min_samples=args.min_samples)
            if "" in tree.children:
                print(tree.children[""].__str__("    ", lambda x: format_capacity(x)))
            else:
                print(tree.__str__("    ", lambda x: format_capacity(x)))

def process_command_line(args):
    parser = ArgumentParser()
    parser.add_argument("-U", "--user", required=True,
            help="The user to connect as (default: %(default)s)")

    parser.add_argument("-P", "--password",
        help="The password for the specified user. If not specified, you will be prompted later.")

    parser.add_argument("-C", "--cluster", default="qumulo",
        help="The hostname of the cluster to connect to (default: %(default)s)")

    parser.add_argument("-p", "--port", type=int, default=8000,
        help="The port to connect to (default: %(default)s)")

    parser.add_argument("-s", "--samples", type=int, default=2000,
        help="The number of samples to take (default: %(default)s)")

    parser.add_argument("-c", "--concurrency", type=int, default=10,
        help="The number of threads to query with (default: %(default)s)")

    parser.add_argument("-m", "--min-samples", type=int, default=5,
        help='''The minimum number of samples to show at a leaf in output
                (default: %(default)s)''')

    parser.add_argument("-x", "--max-leaves", type=int, default=30,
        help='''The maximum number of leaves to show per user
                (default: %(default)s)''')

    parser.add_argument(
        "-D", "--dollars-per-terabyte", type=float,
        help="Show capacity in dollars. Set conversion factor in $/TB/month")

    parser.add_argument("-i", "--confidence-interval", action="store_true",
        help="Show 95%% confidence intervals")

    parser.add_argument("-A", "--allow-self-signed-server", action="store_true",
        help="Silently connect to self-signed servers")

    parser.add_argument("--fancy-output", action="store_true",
        help="Organize each output item into a 'pruned' path tree")

    parser.add_argument("path", help="Filesystem path to sample")

    return parser.parse_args(args)

if __name__ == '__main__':
    main(process_command_line(sys.argv[1:]))
