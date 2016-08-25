from charms.reactive import hook
from charms.reactive import RelationBase
from charms.reactive import scopes
from charms.reactive import is_state


class CephClient(RelationBase):
    scope = scopes.GLOBAL
    auto_accessors = ['key', 'auth', 'public_address']

    @hook('{requires:ceph-admin}-relation-{joined,changed}')
    def changed(self):
        self.set_state('{relation_name}.connected')
        key = None
        auth = None
        public_addr = None

        try:
            key = self.key
        except AttributeError:
            pass

        try:
            auth = self.auth
        except AttributeError:
            pass

        try:
            public_addr = self.public_address
        except AttributeError:
            pass

        data = {
            'key': key,
            'auth': auth,
            'public_address': public_addr
        }

        if all(data.values()):
            self.set_state('{relation_name}.available')

    @hook('{requires:ceph-admin}-relation-{broken,departed}')
    def broken(self):
        if is_state('{relation_name}.available'):
            self.remove_state('{relation_name}.available')
