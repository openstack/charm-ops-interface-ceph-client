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
    ObjectEvents,
    EventSource,
    Object
)

logger = logging.getLogger(__name__)


class BrokerAvailableEvent(EventBase):
    pass


class PoolAvailableEvent(EventBase):
    pass


class CephClientEvents(ObjectEvents):
    broker_available = EventSource(BrokerAvailableEvent)
    pools_available = EventSource(PoolAvailableEvent)


class CephClientRequires(Object):

    on = CephClientEvents()
    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.name = relation_name
        self.this_unit = self.model.unit
        self.relation_name = relation_name
        self.state.set_default(
            pools_available=False,
            broker_req={})
        self.framework.observe(
            charm.on[relation_name].relation_changed,
            self.on_changed)

    def request_osd_settings(self, settings):
        relation = self.model.get_relation(self.relation_name)
        relation.data[self.model.unit]['osd-settings'] = json.dumps(
            settings,
            sort_keys=True)

    @property
    def pools_available(self):
        return self.state.pools_available

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

    def existing_request_complete(self):
        rq = self.get_existing_request()
        if rq and self.is_request_complete(rq,
                                           self.model.relations[self.name]):
            return True
        return False

    def on_changed(self, event):
        logging.info("ceph client on_changed")
        relation_data = self.get_relation_data()
        if relation_data:
            logging.info("emiting broker_available")
            self.on.broker_available.emit()
            if self.existing_request_complete():
                logging.info("emiting pools available")
                self.on.pools_available.emit()
            else:
                logging.info("incomplete request. broker_req not found")

    def get_broker_rsp_key(self):
        return 'broker-rsp-{}'.format(self.this_unit.name.replace('/', '-'))

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
        logging.info("create_replicated_pool: {}".format(relations))
        if not relations:
            return
        rq = self.get_existing_request()
        logging.info("Adding create_replicated_pool request")
        rq.add_op_create_replicated_pool(name=name,
                                         replica_count=replicas,
                                         pg_num=pg_num,
                                         weight=weight,
                                         group=group,
                                         namespace=namespace)
        logging.info("Storing request")
        self.state.broker_req = rq.request
        logging.info("Calling send_request_if_needed")
        # ch_ceph.send_request_if_needed(rq, relation=self.name)
        self.send_request_if_needed(rq, relations)

    def request_ceph_permissions(self, client_name, permissions):
        logging.info("request_ceph_permissions")
        relations = self.framework.model.relations[self.name]
        if not relations:
            return
        rq = self.get_existing_request()
        rq.add_op({'op': 'set-key-permissions',
                   'permissions': permissions,
                   'client': client_name})
        self.state.broker_req = rq.request
        # ch_ceph.send_request_if_needed(rq, relation=self.name)
        self.send_request_if_needed(rq, relations)

    def get_previous_request(self, relation):
        """Get the previous request.

        :param relation: Relation to check for existing request.
        :type relation: ops.model.Relation,
        :returns: The previous ceph request.
        :rtype: ch_ceph.CephBrokerRq
        """
        request = None
        broker_req = relation.data[self.this_unit].get('broker_req')
        if broker_req:
            request_data = json.loads(broker_req)
            request = ch_ceph.CephBrokerRq(
                api_version=request_data['api-version'],
                request_id=request_data['request-id'])
            request.set_ops(request_data['ops'])

        return request

    def get_request_states(self, request, relations):
        """Get the existing requests and their states.

        :param request: A CephBrokerRq object
        :type request: ch_ceph.CephBrokerRq
        :param relations: List of relations to check for existing request.
        :type relations: [ops.model.Relation, ...]
        :returns: Whether request is complete.
        :rtype: bool
        """
        complete = []
        requests = {}
        for relation in relations:
            complete = False
            previous_request = self.get_previous_request(relation)
            if request == previous_request:
                sent = True
                complete = self.is_request_complete_for_relation(
                    previous_request,
                    relation)
            else:
                sent = False
                complete = False

            rid = "{}:{}".format(relation.name, relation.id)
            requests[rid] = {
                'sent': sent,
                'complete': complete,
            }

        return requests

    def is_request_complete_for_relation(self, request, relation):
        """Check if a given request has been completed on the given relation

        :param request: A CephBrokerRq object
        :type request: ch_ceph.CephBrokerRq
        :param relation: A relation to check for an existing request.
        :type relation: ops.model.Relation
        :returns: Whether request is complete.
        :rtype: bool
        """
        broker_key = self.get_broker_rsp_key()
        for unit in relation.units:
            if relation.data[unit].get(broker_key):
                rsp = ch_ceph.CephBrokerRsp(relation.data[unit][broker_key])
                if rsp.request_id == request.request_id:
                    if not rsp.exit_code:
                        return True
            else:
                if relation.data[unit].get('broker_rsp'):
                    logging.info('No response for this unit yet')
        return False

    def is_request_complete(self, request, relations):
        """Check a functionally equivalent request has already been completed

        Returns True if a similair request has been completed

        :param request: A CephBrokerRq object
        :type request: ch_ceph.CephBrokerRq
        :param relations: List of relations to check for existing request.
        :type relations: [ops.model.Relation, ...]
        :returns: Whether request is complete.
        :rtype: bool
        """
        states = self.get_request_states(request, relations)
        for rid in states.keys():
            if not states[rid]['complete']:
                return False

        return True

    def is_request_sent(self, request, relations):
        """Check if a functionally equivalent request has already been sent

        Returns True if a similair request has been sent

        :param request: A CephBrokerRq object
        :type request: ch_ceph.CephBrokerRq
        :param relations: List of relations to check for existing request.
        :type relations: [ops.model.Relation, ...]
        :returns: Whether equivalent request has been sent.
        :rtype: bool
        """
        states = self.get_request_states(request, relations)
        for rid in states.keys():
            if not states[rid]['sent']:
                return False

        return True

    def send_request_if_needed(self, request, relations):
        """Send request if an equivalent request has not already been sent

        :param request: A CephBrokerRq object
        :type request: ch_ceph.CephBrokerRq
        :param relations: List of relations to check for existing request.
        :type relations: [ops.model.Relation, ...]
        """
        if self.is_request_sent(request, relations):
            logging.debug('Request already sent, not sending new request')
        else:
            for relation in relations:
                logging.debug('Sending request {}'.format(request.request_id))
                relation.data[self.this_unit]['broker_req'] = request.request
