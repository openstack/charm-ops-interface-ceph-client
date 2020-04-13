#!/usr/bin/env python3

# Copyright 2020 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import json

from unittest import mock

from ops import framework
from ops.charm import CharmBase
from ops.testing import Harness

from interface_ceph_client import CephClientRequires, BrokerAvailableEvent


class TestCephClientRequires(unittest.TestCase):

    def setUp(self):
        self.harness = Harness(CharmBase, meta='''
            name: client
            provides:
              ceph-client:
                interface: ceph-client
        ''')

    def test_request_osd_settings(self):
        self.harness.begin()
        self.ceph_client = CephClientRequires(self.harness.charm,
                                              'ceph-client')
        relation_id = self.harness.add_relation('ceph-client', 'ceph-mon')
        self.harness.add_relation_unit(
            relation_id,
            'ceph-mon/0',
            {'ingress-address': '192.0.2.2'}
        )
        settings = {
            'osd heartbeat grace': 20,
            'osd heartbeat interval': 5
        }
        self.ceph_client.request_osd_settings(settings)

        rel = self.harness.charm.model.get_relation('ceph-client')
        rel_data = rel.data[self.harness.charm.model.unit]
        self.assertEqual(json.loads(rel_data['osd-settings']), settings)

    def test_mon_hosts(self):
        self.harness.begin()
        self.ceph_client = CephClientRequires(self.harness.charm,
                                              'ceph-client')
        mon_ips = ['192.0.2.1', '192.0.2.2', '2001:DB8::1']
        mon_hosts = self.ceph_client.mon_hosts(mon_ips)
        self.assertEqual(mon_hosts, ['192.0.2.1', '192.0.2.2',
                                     '[2001:DB8::1]'])

    def test_mon_hosts_ceph_proxy(self):
        self.harness.begin()
        self.ceph_client = CephClientRequires(self.harness.charm,
                                              'ceph-client')
        proxy_mon_ips = ['192.0.2.1 192.0.2.2 2001:DB8::1']
        mon_hosts = self.ceph_client.mon_hosts(proxy_mon_ips)
        self.assertEqual(mon_hosts, ['192.0.2.1', '192.0.2.2',
                                     '[2001:DB8::1]'])

    def test_get_relation_data(self):
        relation_id_a = self.harness.add_relation('ceph-client', 'ceph-monA')
        relation_id_b = self.harness.add_relation('ceph-client', 'ceph-monB')
        self.harness.add_relation_unit(
            relation_id_a,
            'ceph-monA/0',
            {'ingress-address': '192.0.2.2',
             'ceph-public-address': '192.0.2.2',
             'key': 'foo',
             'auth': 'bar'},
        )
        self.harness.add_relation_unit(
            relation_id_a,
            'ceph-monA/1',
            {'ingress-address': '192.0.2.3'},
        )
        self.harness.add_relation_unit(
            relation_id_b,
            'ceph-monB/0',
            {'ingress-address': '2001:DB8::1',
             'ceph-public-address': '2001:DB8::1',
             'key': 'foo',
             'auth': 'bar'},
        )
        self.harness.add_relation_unit(
            relation_id_b,
            'ceph-monB/1',
            {'ingress-address': '2001:DB8::2',
             'ceph-public-address': '2001:DB8::2'},
        )

        # TODO: on_changed -> get_pool_data -> is_request_complete ->
        # -> get_request_states -> wrapper -> relation_ids
        # is_request_complete needs to be replaced with something
        # else to be testable with the framework harness.
        # For now the .begin() call is moved to a later point
        # to avoid triggering -changed events since they're not yet fired
        # for initial relation data.
        self.harness.begin()
        self.ceph_client = CephClientRequires(self.harness.charm,
                                              'ceph-client')
        rel_data = self.ceph_client.get_relation_data()
        self.assertEqual(
            rel_data,
            {
                'mon_hosts': ['192.0.2.2', '[2001:DB8::1]', '[2001:DB8::2]'],
                'key': 'foo',
                'auth': 'bar',
            }
        )

    @mock.patch('charmhelpers.contrib.storage.linux.ceph.is_request_complete')
    @mock.patch.object(CephClientRequires, 'get_relation_data')
    def test_get_pool_data(self, _get_relation_data, _is_request_complete):
        # TODO: Replace mocking with real calls once a way to avoid calling
        # hook tools via charmhelpers.hookenv is found.
        relation_data = {'foo': 'bar'}
        _get_relation_data.return_value = relation_data
        _is_request_complete.return_value = True
        self.harness.begin()
        self.ceph_client = CephClientRequires(self.harness.charm,
                                              'ceph-client')

        pool_data = self.ceph_client.get_pool_data()
        self.assertEqual(relation_data, pool_data)

    @mock.patch('charmhelpers.contrib.storage.linux.ceph.is_request_complete')
    @mock.patch.object(CephClientRequires, 'get_relation_data')
    def test_get_pool_data_explicit(self, _get_relation_data,
                                    _is_request_complete):
        # TODO: Replace mocking with real calls once a way to avoid calling
        # hook tools via charmhelpers.hookenv is found.
        relation_data = {'foo': 'bar'}
        _get_relation_data.return_value = relation_data
        _is_request_complete.return_value = True
        self.harness.begin()
        self.ceph_client = CephClientRequires(self.harness.charm,
                                              'ceph-client')
        pool_data = self.ceph_client.get_pool_data({'some': 'data'})
        self.assertEqual(pool_data, {'some': 'data'})

    @mock.patch('charmhelpers.contrib.storage.linux.ceph.is_request_complete')
    @mock.patch.object(CephClientRequires, 'get_relation_data')
    def test_get_pool_data_incomplete(self, _get_relation_data,
                                      _is_request_complete):
        # TODO: Replace mocking with real calls once a way to avoid calling
        # hook tools via charmhelpers.hookenv is found.
        _is_request_complete.return_value = False
        _get_relation_data.return_value = {}
        self.harness.begin()
        self.ceph_client = CephClientRequires(self.harness.charm,
                                              'ceph-client')
        pool_data = self.ceph_client.get_pool_data()
        self.assertEqual(pool_data, None)

    @mock.patch.object(CephClientRequires, 'get_relation_data')
    @mock.patch.object(CephClientRequires, 'get_pool_data')
    def test_on_changed(self, _get_pool_data, _get_relation_data):
        # TODO: Replace mocking with real calls once a way to avoid calling
        # hook tools via charmhelpers.hookenv is found.
        _get_pool_data.return_value = {}
        _get_relation_data.return_value = {}

        class TestReceiver(framework.Object):

            def __init__(self, parent, key):
                super().__init__(parent, key)
                self.observed_events = []

            def on_broker_available(self, event):
                self.observed_events.append(event)

        self.harness.begin()
        self.ceph_client = CephClientRequires(self.harness.charm,
                                              'ceph-client')
        receiver = TestReceiver(self.harness.framework, 'receiver')
        self.harness.framework.observe(self.ceph_client.on.broker_available,
                                       receiver)
        # No data yet.
        relation_id = self.harness.add_relation('ceph-client', 'ceph-mon')
        self.harness.add_relation_unit(
            relation_id,
            'ceph-mon/0',
            {'ingress-address': '192.0.2.2',
             'ceph-public-address': '192.0.2.2'},
        )
        self.assertEqual(len(receiver.observed_events), 0)

        # Got the necessary data - should get a BrokerAvailable event.
        _get_pool_data.return_value = {'foo': 'bar'}
        _get_relation_data.return_value = {'foo': 'bar'}
        self.harness.add_relation_unit(
            relation_id,
            'ceph-mon/0',
            {'ingress-address': '192.0.2.2',
             'ceph-public-address': '192.0.2.2',
             'key': 'foo',
             'auth': 'bar'},
        )
        self.assertEqual(len(receiver.observed_events), 1)
        self.assertIsInstance(receiver.observed_events[0],
                              BrokerAvailableEvent)

    @mock.patch('charmhelpers.contrib.storage.linux.ceph'
                '.send_request_if_needed')
    # Expected failure, need https://github.com/canonical/operator/pull/196
    @unittest.expectedFailure
    def test_create_replicated_pool(self, _send_request_if_needed):
        # TODO: Replace mocking with real calls once a way to avoid calling
        # hook tools via charmhelpers.hookenv is found. Otherwise this test
        # is not very useful.
        self.harness.begin()
        self.ceph_client = CephClientRequires(self.harness.charm,
                                              'ceph-client')

        self.ceph_client.create_replicated_pool('ceph-client')
        _send_request_if_needed.assert_not_called()

        self.harness.add_relation('ceph-client', 'ceph-mon')
        self.ceph_client.create_replicated_pool('ceph-client')
        _send_request_if_needed.assert_called()

    @mock.patch('charmhelpers.contrib.storage.linux.ceph'
                '.send_request_if_needed')
    # Expected failure, need https://github.com/canonical/operator/pull/196
    @unittest.expectedFailure
    def test_create_request_ceph_permissions(self, _send_request_if_needed):
        # TODO: Replace mocking with real calls once a way to avoid calling
        # hook tools via charmhelpers.hookenv is found. Otherwise this test
        # is not very useful.
        self.harness.begin()
        self.ceph_client = CephClientRequires(self.harness.charm,
                                              'ceph-client')
        CEPH_CAPABILITIES = [
            "osd", "allow *",
            "mon", "allow *",
            "mgr", "allow r"
        ]
        self.ceph_client.request_ceph_permissions('ceph-iscsi',
                                                  CEPH_CAPABILITIES)
        _send_request_if_needed.assert_not_called()

        self.harness.add_relation('ceph-client', 'ceph-mon')
        self.ceph_client.create_replicated_pool('ceph-client')
        _send_request_if_needed.assert_called()


if __name__ == '__main__':
    unittest.main()
