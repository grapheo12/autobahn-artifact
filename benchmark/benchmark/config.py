# Copyright(C) Facebook, Inc. and its affiliates.
from json import dump, load
from collections import OrderedDict


class ConfigError(Exception):
    pass


class Key:
    def __init__(self, name, secret):
        self.name = name
        self.secret = secret

    @classmethod
    def from_file(cls, filename):
        assert isinstance(filename, str)
        with open(filename, 'r') as f:
            data = load(f)
        return cls(data['name'], data['secret'])


class Committee:
    ''' The committee looks as follows:
        "authorities: {
            "name": {
                "stake": 1,
                "consensus: {
                    "consensus_to_consensus": x.x.x.x:x,
                },
                "primary: {
                    "primary_to_primary": x.x.x.x:x,
                    "worker_to_primary": x.x.x.x:x,
                },
                "workers": {
                    "0": {
                        "primary_to_worker": x.x.x.x:x,
                        "worker_to_worker": x.x.x.x:x,
                        "transactions": x.x.x.x:x
                    },
                    ...
                }
            },
            ...
        }
    '''

    def __init__(self, ids, addresses, base_port):
        ''' The `addresses` field looks as follows:
            { 
                "name": ["host", "host", ...],
                ...
            }
        '''
        assert isinstance(addresses, OrderedDict)
        assert all(isinstance(x, str) for x in addresses.keys())
        assert all(
            isinstance(x, list) and len(x) > 1 for x in addresses.values()
        )
        assert all(
            isinstance(x, str) for y in addresses.values() for x in y
        )
        assert len({len(x) for x in addresses.values()}) == 1
        assert isinstance(base_port, int) and base_port > 1024

        port = base_port
        self.json = {'authorities': OrderedDict()}
        id_counter = 0

        # Store original addresses for client generation later
        original_addresses = OrderedDict()
        for name, hosts in addresses.items():
            original_addresses[name] = list(hosts)  # Make a copy

        for name, hosts in addresses.items():
            host = hosts.pop(0)
            consensus_addr = {
                'consensus_to_consensus': f'{host}:{port}',
            }
            port += 1

            primary_addr = {
                'primary_to_primary': f'{host}:{port}',
                'worker_to_primary': f'{host}:{port + 1}'
            }
            port += 2

            workers_addr = OrderedDict()
            for j, host in enumerate(hosts):
                workers_addr[j] = {
                    'primary_to_worker': f'{host}:{port}',
                    'transactions': f'{host}:{port + 1}',
                    'worker_to_worker': f'{host}:{port + 2}',
                }
                port += 3

            self.json['authorities'][name] = {
                'stake': 1,
                'id': ids[id_counter],
                'consensus': consensus_addr,
                'primary': primary_addr,
                'workers': workers_addr
            }
            id_counter += 1

        # Add client addresses (one client per worker across all nodes)
        self.json['clients'] = OrderedDict()
        client_id = 0
        for name, hosts in original_addresses.items():
            # Use the primary host for all clients of this node
            primary_host = hosts[0]
            # Number of workers for this node (excluding primary)
            num_workers = len(hosts) - 1
            for worker_idx in range(num_workers):
                self.json['clients'][str(client_id)] = {
                    'replies': f'{primary_host}:{port}'
                }
                port += 1
                client_id += 1

    def primary_addresses(self, faults=0):
        ''' Returns an ordered list of primaries' addresses. '''
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for authority in list(self.json['authorities'].values())[:good_nodes]:
            addresses += [authority['primary']['primary_to_primary']]
        return addresses

    def workers_addresses(self, faults=0):
        ''' Returns an ordered list of list of workers' addresses. '''
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for authority in list(self.json['authorities'].values())[:good_nodes]:
            authority_addresses = []
            for id, worker in authority['workers'].items():
                authority_addresses += [(id, worker['transactions'])]
            addresses.append(authority_addresses)
        return addresses

    def client_addresses(self):
        ''' Returns an ordered list of client reply addresses. '''
        if 'clients' not in self.json:
            return []
        return [client['replies'] for client in self.json['clients'].values()]

    def ips(self, name=None):
        ''' Returns all the ips associated with an authority (in any order). '''
        if name is None:
            names = list(self.json['authorities'].keys())
        else:
            names = [name]

        ips = set()
        for name in names:
            addresses = self.json['authorities'][name]['consensus']
            ips.add(self.ip(addresses['consensus_to_consensus']))

            addresses = self.json['authorities'][name]['primary']
            ips.add(self.ip(addresses['primary_to_primary']))
            ips.add(self.ip(addresses['worker_to_primary']))

            for worker in self.json['authorities'][name]['workers'].values():
                ips.add(self.ip(worker['primary_to_worker']))
                ips.add(self.ip(worker['worker_to_worker']))
                ips.add(self.ip(worker['transactions']))

        return list(ips)

    def remove_nodes(self, nodes):
        ''' remove the `nodes` last nodes from the committee. '''
        assert nodes < self.size()
        for _ in range(nodes):
            self.json['authorities'].popitem()

    def size(self):
        ''' Returns the number of authorities. '''
        return len(self.json['authorities'])

    def workers(self):
        ''' Returns the total number of workers (all authorities altogether). '''
        return sum(len(x['workers']) for x in self.json['authorities'].values())

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)

    @staticmethod
    def ip(address):
        assert isinstance(address, str)
        return address.split(':')[0]


class LocalCommittee(Committee):
    def __init__(self, names, ids, port, workers):
        assert isinstance(names, list)
        assert all(isinstance(x, str) for x in names)
        assert isinstance(port, int)
        assert isinstance(workers, int) and workers > 0
        addresses = OrderedDict((x, ['127.0.0.1']*(1+workers)) for x in names)
        super().__init__(ids, addresses, port)


class NodeParameters:
    def __init__(self, json):
        inputs = []
        try:
            inputs += [json['timeout_delay']]
            inputs += [json['header_size']]
            inputs += [json['max_header_delay']]
            inputs += [json['gc_depth']]
            inputs += [json['sync_retry_delay']]
            inputs += [json['sync_retry_nodes']]
            inputs += [json['batch_size']]
            inputs += [json['max_batch_delay']]
        except KeyError as e:
            raise ConfigError(f'Malformed parameters: missing key {e}')

        if not all(isinstance(x, int) for x in inputs):
            raise ConfigError('Invalid parameters type')

        self.json = json

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)


class BenchParameters:
    def __init__(self, json):
        try:
            self.faults = int(json['faults'])

            nodes = json['nodes']
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes or any(x <= 1 for x in nodes):
                raise ConfigError('Missing or invalid number of nodes')
            self.nodes = [int(x) for x in nodes]

            rate = json['rate']
            rate = rate if isinstance(rate, list) else [rate]
            if not rate:
                raise ConfigError('Missing input rate')
            self.rate = [int(x) for x in rate]

            self.workers = int(json['workers'])
            
            self.worker_fault_tolerance = int(json.get('worker_fault_tolerance', 1))

            if 'collocate' in json:
                self.collocate = bool(json['collocate'])
            else:
                self.collocate = True

            self.tx_size = int(json['tx_size'])

            self.duration = int(json['duration'])

            self.runs = int(json['runs']) if 'runs' in json else 1
            self.simulate_partition = bool(json['simulate_partition'])

            self.partition_nodes = int(json['partition_nodes'])
            self.partition_start = int(json['partition_start'])
            self.partition_duration = int(json['partition_duration'])
        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

        if min(self.nodes) <= self.faults:
            raise ConfigError('There should be more nodes than faults')


class PlotParameters:
    def __init__(self, json):
        try:
            faults = json['faults']
            faults = faults if isinstance(faults, list) else [faults]
            self.faults = [int(x) for x in faults] if faults else [0]

            nodes = json['nodes']
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes:
                raise ConfigError('Missing number of nodes')
            self.nodes = [int(x) for x in nodes]

            workers = json['workers']
            workers = workers if isinstance(workers, list) else [workers]
            if not workers:
                raise ConfigError('Missing number of workers')
            self.workers = [int(x) for x in workers]

            if 'collocate' in json:
                self.collocate = bool(json['collocate'])
            else:
                self.collocate = True

            self.tx_size = int(json['tx_size'])

            max_lat = json['max_latency']
            max_lat = max_lat if isinstance(max_lat, list) else [max_lat]
            if not max_lat:
                raise ConfigError('Missing max latency')
            self.max_latency = [int(x) for x in max_lat]

        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

        if len(self.nodes) > 1 and len(self.workers) > 1:
            raise ConfigError(
                'Either the "nodes" or the "workers can be a list (not both)'
            )

    def scalability(self):
        return len(self.workers) > 1
