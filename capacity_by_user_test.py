#!/usr/bin/env python3

import unittest

from multiprocessing.pool import ThreadPool
from parameterized import parameterized
from typing import Any, Mapping, Sequence
from unittest.mock import MagicMock, patch

from capacity_by_user import (
    Credentials,
    WorkerArgs,
    get_samples,
    parse_args,
    pretty_print_capacity,
    translate_owner_to_owner_string,
)


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
    def setUp(self) -> None:
        self.mock_client = MagicMock()

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

    @patch('capacity_by_user.get_samples_worker')
    def test_get_samples_sums_data_from_workers(self, mock_worker) -> None:
        mock_worker.return_value = 1

        concurrency = 5
        pool = ThreadPool(concurrency)

        credentials = Credentials('my_user', 'my_password', 'my_cluster', 8000)
        samples = 5
        path = 'my_path'
        result = get_samples(pool, credentials, samples, concurrency, path)

        self.assertEqual(mock_worker.call_count, concurrency)
        mock_worker.assert_called_with(
                WorkerArgs(credentials, path, samples / concurrency))
        self.assertEqual(result, concurrency)

    def create_mock_ad_client(self, return_value: Mapping[str, Any]) -> None:
        self.mock_client.ad = MagicMock()
        self.mock_client.ad.sid_to_ad_account = MagicMock(
                return_value=return_value)

    def create_mock_auth_client(self, return_value: Mapping[str, Any]) -> None:
        self.mock_client.auth = MagicMock()
        self.mock_client.auth.auth_id_to_all_related_identities = MagicMock(
                return_value=return_value)

    def test_translate_owner_to_owner_string_happy_path_smb(self) -> None:
        self.owner_value = '1234567'
        return_value = {
            'name': 'cli-user-details-name',
            'classes': []
        }
        self.create_mock_ad_client(return_value)

        result = translate_owner_to_owner_string(
            self.mock_client,
            'unused-auth-id',
            'SMB_SID',
            self.owner_value
        )

        self.mock_client.ad.sid_to_ad_account.assert_called_once()
        self.mock_client.ad.sid_to_ad_account.assert_called_with(
                self.owner_value)
        self.assertEqual(result, 'AD:cli-user-details-name')

    @parameterized.expand([
        [[], 'AD:cli-user-details-name'],
        [['group'], 'NFS:daemon (id:1)'],
    ])
    def test_translate_owner_to_owner_string_happy_path_nfs_to_smb(
        self,
        classes: Sequence[str],
        expected_str: str
    ) -> None:
        self.owner_value = '1'
        ad_return_value = {
            'name': 'cli-user-details-name',
            'classes': classes
        }
        self.create_mock_ad_client(ad_return_value)

        auth_return_value = [
            {
                'id_type': 'SMB_SID',
                'id_value': self.owner_value
            }
        ]
        self.create_mock_auth_client(auth_return_value)

        auth_id = 'auth-id'
        result = translate_owner_to_owner_string(
                self.mock_client, auth_id, 'NFS_UID', self.owner_value)

        self.mock_client.auth.auth_id_to_all_related_identities.assert_called_once()
        self.mock_client.auth.auth_id_to_all_related_identities.assert_called_with(auth_id)
        self.mock_client.ad.sid_to_ad_account.assert_called_once()
        self.mock_client.ad.sid_to_ad_account.assert_called_with(
                self.owner_value)
        self.assertEqual(result, expected_str)

    def test_translate_owner_to_owner_string_local_user_happy_path(
        self,
    ) -> None:
        owner_value = '1234'
        result = translate_owner_to_owner_string(
                self.mock_client, 'unused-auth-id', 'LOCAL_USER', owner_value)

        self.assertEqual(result, f'LOCAL:{owner_value}')

    @parameterized.expand([
        ['SMB_SID', 'smb-owner-value'],
        ['NFS_UID', 'nfs-owner-value'],
        ['FAKE_ID', 'fake-owner-value'],
    ])
    def test_translate_owner_to_owner_string_formats_same_for_all_on_error(
        self,
        owner_type: str,
        owner_value: str
    ) -> None:
        # Cause errors whenever querying the cluster from the RestClient
        self.mock_client.ad.sid_to_ad_account.side_effect = Exception()
        self.mock_client.auth.auth_id_to_all_related_identities.side_effect = Exception()

        result = translate_owner_to_owner_string(
                self.mock_client, 'unused-auth-id', owner_type, owner_value)

        self.assertEqual(result, f'{owner_type}:{owner_value}')


if __name__ == '__main__':
    unittest.main()
