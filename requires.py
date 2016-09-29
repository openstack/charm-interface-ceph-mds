import socket

from charms.reactive import hook, set_state
from charms.reactive import RelationBase
from charms.reactive import scopes
from charms.reactive import is_state
from charmhelpers.core.hookenv import (
    DEBUG)
from charmhelpers.core.hookenv import (
    log, status_set, service_name)
from charmhelpers.contrib.storage.linux.ceph import (
    CephBrokerRq,
    is_request_complete,
    send_request_if_needed,
)


# Each CephFS juju service gets its own set of data/metadata pools
def create_broker_requests():
    log("Creating broker request")
    name = socket.gethostname()
    rq = CephBrokerRq()
    rq.add_op_create_pool(name="{}_data".format(service_name()),
                          replica_count=3,
                          weight=None)
    rq.add_op_create_pool(name="{}_metadata".format(service_name()),
                          replica_count=3,
                          weight=None)
    # Create CephFS
    rq.ops.append(
        {
            'op': 'create-cephfs',
            'mds_name': name,
            'data_pool': "data_{}".format(name),
            'metadata_pool': "metadata_{}".format(name),
        }
    )
    return rq


def async_pool_request():
    status_set('maintenance', "Requesting CephFS pool creation")
    rq = create_broker_requests()

    if is_request_complete(rq, relation='mds'):
        log('Broker request complete', level=DEBUG)
        set_state("cephfs.pools.created")
        status_set('maintenance', "CephFS pools created")
    else:
        log('sending broker request: {}'.format(rq),
            level=DEBUG)
        send_request_if_needed(rq, relation='mds')


class CephClient(RelationBase):
    scope = scopes.GLOBAL
    auto_accessors = ['key', 'fsid', 'auth',
                      'ceph-public-address']

    @hook('{requires:ceph-mds}-relation-{joined,changed}')
    def changed(self):
        self.set_remote(key='mds-name', value=socket.gethostname())
        self.set_state('{relation_name}.connected')
        key = None
        fsid = None
        auth = None
        ceph_public_address = None

        try:
            key = self.key
        except AttributeError:
            pass

        try:
            fsid = self.fsid
        except AttributeError:
            pass

        try:
            auth = self.auth
        except AttributeError:
            pass

        try:
            ceph_public_address = self.ceph_public_address
        except AttributeError:
            pass

        data = {
            'key': key,
            'fsid': fsid,
            'auth': auth,
            'ceph_public_address': ceph_public_address
        }

        if all(data.values()):
            self.set_state('{relation_name}.available')
            async_pool_request()

    @hook('{requires:ceph-mds}-relation-{broken,departed}')
    def broken(self):
        if is_state('{relation_name}.available'):
            self.remove_state('{relation_name}.available')
