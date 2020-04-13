#!/usr/bin/env python3

import logging
import json
import sys
sys.path.append('lib') # noqa

import charmhelpers.contrib.storage.linux.ceph as ch_ceph
import charmhelpers.contrib.network.ip as ch_ip

from ops.framework import (
    StoredState,
    EventBase,
    EventSetBase,
    EventSource,
    Object
)

logger = logging.getLogger(__name__)


class BrokerAvailableEvent(EventBase):
    pass


class PoolAvailableEvent(EventBase):
    pass


class CephClientEvents(EventSetBase):
    broker_available = EventSource(BrokerAvailableEvent)
    pools_available = EventSource(PoolAvailableEvent)


class CephClientRequires(Object):

    on = CephClientEvents()
    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.name = relation_name
        self.relation_name = relation_name
        self.state.set_default(broker_req={})
        self.framework.observe(
            charm.on[relation_name].relation_changed,
            self.on_changed)

    def request_osd_settings(self, settings):
        relation = self.model.get_relation(self.relation_name)
        relation.data[self.model.unit]['osd-settings'] = json.dumps(
            settings,
            sort_keys=True)

    def mon_hosts(self, mon_ips):
        """List of all monitor host public addresses"""
        hosts = []
        for ceph_addrs in mon_ips:
            # NOTE(jamespage): This looks odd but deals with
            #                  use with ceph-proxy which
            #                  presents all monitors in
            #                  a single space delimited field.
            for addr in ceph_addrs.split(' '):
                hosts.append(ch_ip.format_ipv6_addr(addr) or addr)
        hosts.sort()
        return hosts

    def get_relation_data(self):
        data = {}
        mon_ips = []
        for relation in self.framework.model.relations[self.relation_name]:
            for unit in relation.units:
                _data = {
                    'key': relation.data[unit].get('key'),
                    'auth': relation.data[unit].get('auth')}
                mon_ip = relation.data[unit].get('ceph-public-address')
                if mon_ip:
                    mon_ips.append(mon_ip)
                if all(_data.values()):
                    data = _data
        if data:
            data['mon_hosts'] = self.mon_hosts(mon_ips)
        return data

    def get_pool_data(self, relation_data=None):
        if not relation_data:
            relation_data = self.get_relation_data()
        rq = self.get_existing_request()
        if rq and ch_ceph.is_request_complete(rq,
                                              relation=self.relation_name):
            return relation_data

    def on_changed(self, event):
        logging.info("ceph client on_changed")
        relation_data = self.get_relation_data()
        if relation_data:
            logging.info("emiting broker_available")
            self.on.broker_available.emit()
            if self.get_pool_data(relation_data=relation_data):
                logging.info("emiting pools available")
                self.on.pools_available.emit()
            else:
                logging.info("incomplete request. broker_req not found")

    def get_existing_request(self):
        logging.info("get_existing_request")
        # json.dumps of the CephBrokerRq()
        rq = ch_ceph.CephBrokerRq()

        if self.state.broker_req:
            try:
                j = json.loads(self.state.broker_req)
                logging.info("Json request: {}".format(self.state.broker_req))
                rq.set_ops(j['ops'])
            except ValueError as err:
                logging.info("Unable to decode broker_req: {}. Error {}"
                             "".format(self.state.broker_req, err))
        return rq

    def create_replicated_pool(self, name, replicas=3, weight=None,
                               pg_num=None, group=None, namespace=None):
        """
        Request pool setup
        @param name: Name of pool to create
        @param replicas: Number of replicas for supporting pools
        @param weight: The percentage of data the pool makes up
        @param pg_num: If not provided, this value will be calculated by the
                       broker based on how many OSDs are in the cluster at the
                       time of creation. Note that, if provided, this value
                       will be capped at the current available maximum.
        @param group: Group to add pool to.
        @param namespace: A group can optionally have a namespace defined that
                          will be used to further restrict pool access.
        """
        logging.info("create_replicated_pool")
        relations = self.framework.model.relations[self.name]
        if not relations:
            return
        rq = self.get_existing_request()
        rq.add_op_create_replicated_pool(name=name,
                                         replica_count=replicas,
                                         pg_num=pg_num,
                                         weight=weight,
                                         group=group,
                                         namespace=namespace)
        self.state.broker_req = rq.request
        ch_ceph.send_request_if_needed(rq, relation=self.name)

    def request_ceph_permissions(self, client_name, permissions):
        logging.info("request_ceph_permissions")
        relations = self.framework.model.relations[self.name]
        if not relations:
            return
        rq = self.get_existing_request()
        rq.add_op({'op': 'set-key-permissions',
                   'permissions': permissions,
                   'client': 'ceph-iscsi'})
        self.state.broker_req = rq.request
        ch_ceph.send_request_if_needed(rq, relation=self.name)
