#!/usr/bin/env python3

import unittest

from parameterized import parameterized

from capacity_by_user import parse_args, pretty_print_capacity


class ArgparseTest(unittest.TestCase):
    def test_default_arguments(self) -> None:
        args = parse_args(['my_path'])
        self.assertEqual(args.user, 'admin')
        self.assertEqual(args.password, 'admin')
        self.assertEqual(args.cluster, 'qumulo')
        self.assertEqual(args.port, 8000)
        self.assertEqual(args.samples, 2000)
        self.assertEqual(args.concurrency, 10)
        self.assertEqual(args.min_samples, 5)
        self.assertEqual(args.max_leaves, 30)
        self.assertIsNone(args.dollars_per_terabyte)
        self.assertFalse(args.confidence_interval)
        self.assertFalse(args.allow_self_signed_server)

    @parameterized.expand([['-u'], ['--user']])
    def test_user(self, user_arg: str) -> None:
        args = parse_args(['my_path', user_arg, 'my_user'])
        self.assertEqual(args.user ,'my_user')

    @parameterized.expand([['-p'], ['--password']])
    def test_password(self, password_arg: str) -> None:
        args = parse_args(['my_path', password_arg, 'my_password'])
        self.assertEqual(args.password ,'my_password')

    @parameterized.expand([['-c'], ['--cluster']])
    def test_cluster(self, cluster_arg: str) -> None:
        args = parse_args(['my_path', cluster_arg, 'my_cluster'])
        self.assertEqual(args.cluster, 'my_cluster')

    @parameterized.expand([['-P'], ['--port']])
    def test_port(self, port_arg: str) -> None:
        args = parse_args(['my_path', port_arg, '8080'])
        self.assertEqual(args.port, 8080)

    @parameterized.expand([['-s'], ['--samples']])
    def test_samples(self, samples_arg: str) -> None:
        args = parse_args(['my_path', samples_arg, '1337'])
        self.assertEqual(args.samples, 1337)

    @parameterized.expand([['-C'], ['--concurrency']])
    def test_concurrency(self, concurrency_arg: str) -> None:
        args = parse_args(['my_path', concurrency_arg, '13'])
        self.assertEqual(args.concurrency, 13)

    @parameterized.expand([['-m'], ['--min-samples']])
    def test_min_samples(self, min_samples_arg: str) -> None:
        args = parse_args(['my_path', min_samples_arg, '7'])
        self.assertEqual(args.min_samples, 7)

    @parameterized.expand([['-x'], ['--max-leaves']])
    def test_max_leaves(self, max_leaves_arg: str) -> None:
        args = parse_args(['my_path', max_leaves_arg, '21'])
        self.assertEqual(args.max_leaves, 21)

    @parameterized.expand([['-D'], ['--dollars-per-terabyte']])
    def test_dollars_per_terabyte(self, dollars_per_terabyte_arg: str) -> None:
        args = parse_args(['my_path', dollars_per_terabyte_arg, '0.45'])
        self.assertEqual(args.dollars_per_terabyte, 0.45)

    @parameterized.expand([['-i'], ['--confidence-interval']])
    def test_confidence_interval(self, confidence_interval_arg: str) -> None:
        args = parse_args(['my_path', confidence_interval_arg])
        self.assertTrue(args.confidence_interval)

    @parameterized.expand([['-A'], ['--allow-self-signed-server']])
    def test_allow_self_signed_server(
        self,
        allow_self_signed_server_arg: str
    ) -> None:
        args = parse_args(['my_path', allow_self_signed_server_arg])
        self.assertTrue(args.allow_self_signed_server)


class HelperTest(unittest.TestCase):
    @parameterized.expand([
        [2 ** 0, 'b'],
        [2 ** 10, 'K'],
        [2 ** 20, 'M'],
        [2 ** 30, 'G'],
        [2 ** 40, 'T'],
        [2 ** 50, 'P'],
        [2 ** 60, 'E'],
    ])
    def test_pretty_print_capacity(self, capacity, expected_unit) -> None:
        self.assertIn(expected_unit, pretty_print_capacity(capacity))



if __name__ == '__main__':
    unittest.main()
