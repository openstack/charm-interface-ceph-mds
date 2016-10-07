import json

from charms.reactive import hook
from charms.reactive import RelationBase
from charms.reactive import scopes
from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import log, local_unit
from charmhelpers.contrib.storage.linux.ceph import (
    CephBrokerRq,
    is_request_complete,
    send_request_if_needed,
)


class CephClient(RelationBase):
    scope = scopes.GLOBAL

    auto_accessors = ['mds_key', 'fsid', 'auth']

    @hook('{requires:ceph-mds}-relation-{joined}')
    def joined(self):
        self.set_remote(key='mds-name', value=socket.gethostname())
        self.set_state('{relation_name}.connected')
        self.initialize_mds(name=local_unit())

    @hook('{requires:ceph-mds}-relation-{changed,departed}')
    def changed(self):
        data = {
            'mds_key': self.mds_key(),
            'fsid': self.fsid(),
            'auth': self.auth(),
            'mon_hosts': self.mon_hosts()
        }
        if all(data.values()):
            self.set_state('{relation_name}.available')

        rq = self.get_local(key='broker_req')
        if rq and is_request_complete(rq,
                                      relation=self.relation_name):
            self.set_state('{relation_name}.pools.available')

    @hook('{requires:ceph-mds}-relation-{broken}')
    def broken(self):
        self.remove_state('{relation_name}.available')
        self.remove_state('{relation_name}.connected')
        self.remove_state('{relation_name}.pools.available')

    def initialize_mds(self, name, replicas=3):
        """
        Request pool setup and mds creation

        @param name: name of mds pools to create
        @param replicas: number of replicas for supporting pools
        """
        # json.dumps of the CephBrokerRq()
        json_rq = self.get_local(key='broker_req')

        if not json_rq:
            rq = CephBrokerRq()
            rq.add_op_create_pool(name="{}_data".format(name),
                                  replica_count=replicas,
                                  weight=None)
            rq.add_op_create_pool(name="{}_metadata".format(name),
                                  replica_count=replicas,
                                  weight=None)
            # Create CephFS
            rq.ops.append({
                'op': 'create-cephfs',
                'mds_name': name,
                'data_pool': "{}_data".format(name),
                'metadata_pool': "{}_metadata".format(name),
            })
            self.set_local(key='broker_req', value=rq.request)
            send_request_if_needed(rq, relation=self.relation_name)
        else:
            rq = CephBrokerRq()
            try:
                j = json.loads(json_rq)
                log("Json request: {}".format(json_rq))
                rq.ops = j['ops']
                send_request_if_needed(rq, relation=self.relation_name)
            except ValueError as err:
                log("Unable to decode broker_req: {}.  Error: {}".format(
                    json_rq, err))

    def get_remote_all(self, key, default=None):
        """Return a list of all values presented by remote units for key"""
        # TODO: might be a nicer way todo this - written a while back!
        values = []
        for conversation in self.conversations():
            for relation_id in conversation.relation_ids:
                for unit in hookenv.related_units(relation_id):
                    value = hookenv.relation_get(key,
                                                 unit,
                                                 relation_id) or default
                    if value:
                        values.append(value)
        return list(set(values))

    def mon_hosts(self):
        """List of all monitor host public addresses"""
        hosts = []
        addrs = self.get_remote_all('ceph-public-address')
        for addr in addrs:
            hosts.append('{}:6789'.format(addr))

        hosts.sort()
        return hosts
