# Copyright 2022 Canonical Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

import lib.policies as rabbit_policies

from subprocess import CalledProcessError
from unittest import mock

from unit_tests.test_utils import CharmTestCase


TO_PATCH = ["check_output", "cmp_pkgrevno", "log", "status_set"]


class PoliciesTests(CharmTestCase):
    def setUp(self):
        super(PoliciesTests, self).setUp(rabbit_policies, TO_PATCH)

    def tearDown(self):
        super(PoliciesTests, self).tearDown()

    def test_policies_classes(self):
        # two classes are equal if they have the same vhost and name
        self.assertEqual(
            rabbit_policies.BasePolicy("/", "HA"),
            rabbit_policies.HAPolicy("/", "HA", "foo"),
        )
        self.assertNotEqual(
            rabbit_policies.BasePolicy("/", "HA"),
            rabbit_policies.HAPolicy("openstack", "HA", "foo"),
        )

        self.assertTrue(
            len(
                set(
                    [
                        rabbit_policies.BasePolicy("/", "HA"),
                        rabbit_policies.HAPolicy("/", "HA", "foo"),
                    ]
                )
            )
            == 1
        )

    def test_set_policy(self):
        generic_policy = rabbit_policies.BasePolicy(
            "/",
            "HA",
            "^(?!amq\\.).*",
            '{"ha-mode": "all", "ha-sync-mode": "automatic"}',
        )
        generic_policy.set()
        expected_call = [
            mock.call(
                [
                    "rabbitmqctl",
                    "set_policy",
                    "-p",
                    "/",
                    "HA",
                    "^(?!amq\\.).*",
                    '{"ha-mode": "all", "ha-sync-mode": "automatic"}',
                    "--apply-to",
                    "queues",
                    "--priority",
                    "1",
                ]
            )
        ]
        self.assertEqual(expected_call, self.check_output.call_args_list)
        self.assertEqual(2, len(self.log.mock_calls))
        self.assertFalse(self.status_set.called)

    def test_set_policy_process_error(self):
        self.check_output.side_effect = CalledProcessError(1, "cmd")
        generic_policy = rabbit_policies.BasePolicy(
            "/",
            "HA",
            "^(?!amq\\.).*",
            '{"ha-mode": "all", "ha-sync-mode": "automatic"}',
        )
        generic_policy.set()
        self.assertEqual(2, len(self.log.mock_calls))
        self.assertTrue(self.status_set.called)

    def test_set_policy_value_error(self):
        self.check_output.side_effect = ValueError
        generic_policy = rabbit_policies.BasePolicy(
            "/",
            "HA",
            "^(?!amq\\.).*",
            '{"ha-mode": "all", "ha-sync-mode": "automatic"}',
        )
        generic_policy.set()
        self.assertEqual(2, len(self.log.mock_calls))
        self.assertTrue(self.status_set.called)

    def test_clear_policy(self):
        generic_policy = rabbit_policies.BasePolicy("/", "HA")
        generic_policy.clear()
        expected_call = [
            mock.call(["rabbitmqctl", "clear_policy", "-p", "/", "HA"])
        ]
        self.assertEqual(expected_call, self.check_output.call_args_list)
        self.assertEqual(2, len(self.log.mock_calls))
        self.assertFalse(self.status_set.called)

    def test_clear_policy_error(self):
        self.check_output.side_effect = CalledProcessError(1, "cmd")
        generic_policy = rabbit_policies.BasePolicy("/", "HA")
        generic_policy.clear()
        self.assertEqual(2, len(self.log.mock_calls))
        self.assertTrue(self.status_set.called)

    def test_wrong_apply_to(self):
        dummy_ttl_config = {
            "vhost": "openstack",
            "name": "TTL",
            "pattern": "foo",
            "apply_to": "bar"
        }
        with self.assertRaises(ValueError):
            rabbit_policies.TTLPolicy(**dummy_ttl_config)

    def test_ttl_policy(self):
        # create a standard ttl policy
        dummy_ttl_config = {
            "vhost": "openstack",
            "name": "TTL",
            "pattern": "foo",
        }
        ttl_policy = rabbit_policies.TTLPolicy(**dummy_ttl_config)
        self.assertEqual(ttl_policy.apply_to, "queues")
        self.assertEqual(ttl_policy.priority, "1")
        self.assertEqual(ttl_policy.type, "ttl")
        self.assertEqual(ttl_policy.ttl, 3600000)
        self.assertDictEqual(
            json.loads(ttl_policy.definition), {"message-ttl": ttl_policy.ttl}
        )

        # changing the field message_ttl, changes definition
        dummy_ttl_config = {
            "vhost": "openstack",
            "name": "TTL",
            "pattern": "foo",
            "apply_to": "all",
            "priority": 0,
            "ttl": "3600",
            "message_ttl": False,
        }
        ttl_policy = rabbit_policies.TTLPolicy(**dummy_ttl_config)
        self.assertEqual(ttl_policy.apply_to, "all")
        self.assertEqual(ttl_policy.priority, "0")
        self.assertEqual(ttl_policy.type, "ttl")
        self.assertEqual(ttl_policy.ttl, 3600)
        self.assertDictEqual(
            json.loads(ttl_policy.definition), {"experies": ttl_policy.ttl}
        )

    def test_ha_policy(self):
        # create a standard ha policy
        dummy_ha_config = {
            "vhost": "openstack",
            "name": "HA",
            "pattern": "foo"
        }
        ha_policy = rabbit_policies.HAPolicy(**dummy_ha_config)
        self.assertEqual(ha_policy.apply_to, "queues")
        self.assertEqual(ha_policy.priority, "1")
        self.assertEqual(ha_policy.type, "ha")
        self.assertEqual(ha_policy.mode, "all")
        self.assertEqual(ha_policy.sync_mode, "automatic")
        self.assertDictEqual(
            json.loads(ha_policy.definition),
            {"ha-mode": "all", "ha-sync-mode": ha_policy.sync_mode}
        )

        # RabbitMQ version accepts ha policy
        self.cmp_pkgrevno.return_value = 1
        ha_policy.set()
        self.assertTrue(self.check_output.called)
        self.assertTrue(ha_policy.check())
        # reset check_output mock
        self.check_output.reset_mock()

        # create a not standard ha policy with mode exactly
        dummy_ha_config = {
            "vhost": "openstack",
            "name": "HA",
            "pattern": "foo",
            "apply_to": "exchanges",
            "priority": 0,
            "params": "bar",
            "mode": "exactly",
            "sync_mode": "manual",
        }
        ha_policy = rabbit_policies.HAPolicy(**dummy_ha_config)
        self.assertEqual(ha_policy.apply_to, "exchanges")
        self.assertEqual(ha_policy.priority, "0")
        self.assertEqual(ha_policy.type, "ha")
        self.assertEqual(ha_policy.mode, "exactly")
        self.assertEqual(ha_policy.sync_mode, "manual")
        self.assertDictEqual(
            json.loads(ha_policy.definition),
            {
                "ha-mode": "exactly",
                "ha-params": ha_policy.params,
                "ha-sync-mode": ha_policy.sync_mode,
            }
        )

        # changing mode to nodes
        ha_policy.mode = "nodes"
        self.assertDictEqual(
            json.loads(ha_policy.definition),
            {
                "ha-mode": "nodes",
                "ha-params": ha_policy.params,
                "ha-sync-mode": ha_policy.sync_mode,
            }
        )

        # raises ValueError if sync_mode is unknown
        with self.assertRaises(ValueError):
            ha_policy.sync_mode = 'foo'

        # raises ValueError if mode is unknown
        with self.assertRaises(ValueError):
            ha_policy.mode = 'bar'

        # RabbitMQ version doesn't accept ha policy
        self.cmp_pkgrevno.return_value = -1
        ha_policy.set()
        self.assertFalse(self.check_output.called)
        self.assertFalse(ha_policy.check())
