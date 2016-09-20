import socket
from charms.reactive import hook
from charms.reactive import RelationBase
from charms.reactive import scopes
from charms.reactive import is_state


class CephClient(RelationBase):
    scope = scopes.GLOBAL
    auto_accessors = ['key', 'fsid', 'auth', 'mon_hosts', 'admin_key']

    @hook('{requires:ceph-mds}-relation-{joined,changed}')
    def changed(self):
        self.set_remote(key='mds-name', value=socket.gethostname())
        self.set_state('{relation_name}.connected')
        admin_key = None
        key = None
        fsid = None
        auth = None
        mon_hosts = None

        try:
            key = self.key
        except AttributeError:
            pass

        try:
            admin_key = self.admin_key
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
            mon_hosts = self.mon_hosts
        except AttributeError:
            pass

        data = {
            'key': key,
            'admin_key': admin_key,
            'fsid': fsid,
            'auth': auth,
            'mon_hosts': mon_hosts
        }

        if all(data.values()):
            self.set_state('{relation_name}.available')

    @hook('{requires:ceph-mds}-relation-{broken,departed}')
    def broken(self):
        if is_state('{relation_name}.available'):
            self.remove_state('{relation_name}.available')
