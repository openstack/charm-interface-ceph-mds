from charms.reactive import hook
from charms.reactive import RelationBase
from charms.reactive import scopes
from charms.reactive import is_state


class CephClient(RelationBase):
    scope = scopes.GLOBAL
    auto_accessors = ['key', 'fsid', 'auth', 'mon_hosts']

    @hook('{requires:ceph-admin}-relation-{joined,changed}')
    def changed(self):
        self.set_state('{relation_name}.connected')
        key = None
        fsid = None
        auth = None
        mon_hosts = None

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
            mon_hosts = self.mon_hosts
        except AttributeError:
            pass

        data = {
            'key': key,
            'fsid': fsid,
            'auth': auth,
            'mon_hosts': mon_hosts
        }

        if all(data.values()):
            self.set_state('{relation_name}.available')

    @hook('{requires:ceph-admin}-relation-{broken,departed}')
    def broken(self):
        if is_state('{relation_name}.available'):
            self.remove_state('{relation_name}.available')
