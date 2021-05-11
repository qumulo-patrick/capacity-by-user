#!/usr/bin/env python3

import argparse
import itertools
import os
import pwd
import sys
import ssl

from dataclasses import dataclass
from functools import cmp_to_key
from operator import attrgetter
from multiprocessing import Pool
from typing import Any, Mapping, Sequence

from qumulo.rest_client import RestClient
from sample_tree_node import SampleTreeNode


def parse_args(args: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Show an overview of capacity consumed by user and path',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-u',
        '--user',
        default='admin',
        help='The user to connect as'
    )
    parser.add_argument(
        '-p',
        '--password',
        default='admin',
        help='The password to connect with'
    )
    parser.add_argument(
        '-c',
        '--cluster',
        default='qumulo',
        help='The hostname of the cluster to connect to'
    )
    parser.add_argument(
        '-P',
        '--port',
        type=int,
        default=8000,
        help='The port to connect to'
    )
    parser.add_argument(
        '-s',
        '--samples',
        type=int,
        default=2000,
        help='The number of samples to take'
    )
    parser.add_argument(
        '-C',
        '--concurrency',
        type=int,
        default=10,
        help='The number of threads to query with'
    )
    parser.add_argument(
        '-m',
        '--min-samples',
        type=int,
        default=5,
        help='The minimum number of samples to show at a leaf in output'
    )
    parser.add_argument(
        '-x',
        '--max-leaves',
        type=int,
        default=30,
        help='The maximum number of leaves to show per user'
    )
    parser.add_argument(
        '-D',
        '--dollars-per-terabyte',
        type=float,
        help='Show capacity in dollars. Set conversion factor in $/TB/month'
    )
    parser.add_argument(
        '-i',
        '--confidence-interval',
        action='store_true',
        help='Show 95%% confidence intervals'
    )
    parser.add_argument(
        '-A',
        '--allow-self-signed-server',
        action='store_true',
        help='Silently connect to self-signed servers'
    )
    parser.add_argument(
        'path',
        help='Filesystem path to sample'
    )

    return parser.parse_args(args)


def pretty_print_capacity(capacity: int) -> str:
    starting_points = [1024 ** k for k in (6, 5, 4, 3, 2, 1, 0)]
    units = ['E', 'P', 'T', 'G', 'M', 'K', 'b']

    for starting_point, unit in zip(starting_points, units):
        if capacity >= starting_point:
            return f'{capacity / float(starting_point):.2f}{unit}'

    return '0'


@dataclass
class Credentials:
    user: str
    password: str
    cluster: str
    port: int


@dataclass
class WorkerArgs:
    credentials: Credentials
    path: str
    samples_to_request: int


def get_samples_worker(args: WorkerArgs) -> int:
    client = RestClient(args.credentials.cluster, args.credentials.port)
    client.login(args.credentials.user, args.credentials.password)
    return client.fs.get_file_samples(
            path=args.path, count=args.samples_to_request, by_value='capacity')


def get_samples(
    pool: Pool,
    credentials: Credentials,
    samples: int,
    concurrency: int,
    path: str
) -> Sequence[Mapping[str, Any]]:
    samples_to_request = samples / concurrency
    request = WorkerArgs(credentials, path, samples_to_request)
    requests = [request] * concurrency

    tiered_results = pool.map(get_samples_worker, requests)

    # Flatten worker output into a single list
    return list(itertools.chain.from_iterable(tiered_results))


def translate_owner_to_owner_string(
    rest_client: RestClient,
    auth_id: str,
    owner_type: str,
    owner_value: str
) -> str:
    user = None

    if owner_type == 'SMB_SID':
        try:
            user_details = rest_client.ad.sid_to_ad_account(owner_value)
            user = f'AD:{user_details["name"]}'
        except:
            pass
    elif owner_type == 'NFS_UID':
        try:
            ids = rest_client.auth.auth_id_to_all_related_identities(auth_id)
        except:
            ids = []
        for i, el in enumerate(ids):
            if el['id_type'] == 'SMB_SID':
                try:
                    user_details = rest_client.ad.sid_to_ad_account(el['id_value'])
                    if 'group' in user_details['classes']:
                        continue
                    user = f'AD:{user_details["name"]}'
                except:
                    continue
        if user == None:
            try:
                pw_name = pwd.getpwuid(int(owner_value)).pw_name
                user = f'NFS:{pw_name} (id:{owner_value})'
            except:
                pass
    elif owner_type == 'LOCAL_USER':
        user = f'LOCAL:{owner_value}'

    if user == None:
        user = f'{owner_type}:{owner_value}'
    return user


seen = {}
def get_file_attrs(
    rest_client: RestClient,
    paths: Sequence[str]
) -> Sequence[str]:
    result = []
    for path in paths:
        if path in seen:
            result += [seen[path]]
            continue
        attrs = rest_client.fs.get_file_attr(path)
        str_owner = translate_owner_to_owner_string(
            rest_client,
            attrs['owner'],
            attrs['owner_details']['id_type'],
            attrs['owner_details']['id_value']
        )
        seen[path] = str_owner
        result.append(str_owner)
    return result


def get_owner_vec(
    pool: Pool,
    rest_client: RestClient,
    samples: Sequence[Mapping[str, Any]],
    num_samples: int
) -> Sequence[str]:
    file_ids = [s['id'] for s in samples]
    sublists = [
        (rest_client.clone(), file_ids[i:i+100])
        for i in range(0, num_samples, 100)
    ]

    tiered_results = pool.starmap(get_file_attrs, sublists)

    # Flatten worker output into a single list
    return list(itertools.chain.from_iterable(tiered_results))


def format_capacity(
    sample_str: str,
    num_samples: int,
    total_capacity_used: float,
    dollars_per_terabyte: float,
    use_confidence_interval: bool
) -> str:
    sample = float(sample_str)
    mean = sample / num_samples
    stddev = (((1 - mean) ** 2 * sample +
                (mean ** 2) * (num_samples - sample)) / num_samples) ** (1/2.)
    confidence =  1.96 * stddev / (num_samples ** (1/2.))

    bytes_per_terabyte = 1000. ** 4
    if dollars_per_terabyte != None:
        def to_dollars(adjust: float) -> float:
            return (
                (mean + adjust)
                * total_capacity_used
                / bytes_per_terabyte
                * dollars_per_terabyte
            )
        if use_confidence_interval:
            low = to_dollars(-confidence)
            high = to_dollars(confidence)
            return f'[${low:.02f} - ${high:.02f}]/month'
        else:
            return f'${to_dollars(0):.02f}/month'
    else:
        if use_confidence_interval:
            low = (mean - confidence) * total_capacity_used
            high = (mean + confidence) * total_capacity_used
            low_str = pretty_print_capacity(low)
            high_str = pretty_print_capacity(high)
            return f'[{low_str} - {high_str}]'
        else:
            return f'{pretty_print_capacity((mean) * total_capacity_used)}'


def main(args: Sequence[str]):
    args = parse_args(args)
    credentials = Credentials(
            args.user, args.password, args.cluster, args.port)

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
    client.login(args.user, args.password)

    total_capacity_used = int(
        client.fs.read_dir_aggregates(args.path)['total_capacity'])

    pool = Pool(args.concurrency)

    # First build a vector of all samples...
    samples = get_samples(
            pool, credentials, args.samples, args.concurrency, args.path)

    # Then get a corresponding vector of owner strings
    owner_vec = get_owner_vec(pool, rest_client, samples, args.samples)

    owners = {}
    directories = {}

    # Create a mapping of user to tree...
    for s, owner in zip(samples, owner_vec):
        owners.setdefault(owner, SampleTreeNode(""))
        owners[owner].insert(s["name"], 1)

    print("Total: %s" % (format_capacity(
        args.samples,
        args.samples,
        args.dollars_per_terabyte,
        args.confidence_interval
    )))
    sort_fn = lambda x, y: y[1].sum_samples - x[1].sum_samples
    sorted_owners = sorted(owners.items(), ket=cmp_to_key(sort_fn))

    # For each owner, print total used, then refine the tree and dump it.
    for name, tree in sorted_owners:
        print("Owner %s (~%0.1f%%/%s)" % (
            name, tree.sum_samples / float(args.samples) * 100,
            format_capacity(
                tree.sum_samples,
                args.samples,
                args.dollars_per_terabyte,
                args.confidence_interval
            )))
        tree.prune_until(max_leaves=args.max_leaves,
                         min_samples=args.min_samples)
        if "" in tree.children:
            print(tree.children[""].__str__(
                "    ",
                lambda x: format_capacity(
                    x,
                    args.samples,
                    args.dollars_per_terabyte,
                    args.confidence_interval
                )
            ))
        else:
            print(tree.__str__(
                "    ",
                lambda x: format_capacity(
                    x,
                    args.samples,
                    args.dollars_per_terabyte,
                    args.confidence_interval
                )
            ))


if __name__ == '__main__':
    main(sys.argv[1:])
