from collections import OrderedDict
from typing import List, Tuple
from benchmark.config import Committee
from benchmark.local import PathMaker, CommandMaker, Key, NodeParameters
import subprocess
import click

class RemoteCommittee(Committee):
    def __init__(self, names, port, workers, ip_list):
        assert isinstance(names, list)
        assert all(isinstance(x, str) for x in names)
        assert isinstance(port, int)
        assert isinstance(workers, int) and workers > 0
        assert len(names) <= len(ip_list)
 
        addresses = OrderedDict((x, [ip_list[i]]*(1+workers)) for i, x in enumerate(names))
        super().__init__(addresses, port)


def gen_config(nodes: int, base_port: int, workers: int, node_parameters: NodeParameters, ip_list: List[str]):
    # Generate configuration files.
    keys = []
    key_files = [PathMaker.key_file(i) for i in range(nodes)]
    for filename in key_files:
        cmd = CommandMaker.generate_key_from_target(filename).split()
        subprocess.run(cmd, check=True)
        keys += [Key.from_file(filename)]

    names = [x.name for x in keys]
    #print('num workers', self.workers)
    committee = RemoteCommittee(names, base_port, workers, ip_list)
    committee.print(PathMaker.committee_file())

    node_parameters.print(PathMaker.parameters_file())


# All gen_*_nodelist functions return: dict(node -> (ip, domain name)) and number of clients
def gen_cluster_nodelist(ip_list, domain_suffix, cnt_start=0, max_nodes=-1) -> Tuple[OrderedDict[str, Tuple[str, str]], int]:
    if ip_list == "/dev/null":
        raise Exception("Ip list must be provided when using cluster mode")
    
    nodelist = OrderedDict()
    node_cnt = cnt_start
    client_cnt = 0
    with open(ip_list) as f:
        for line in f.readlines():
            # Terraform generates VM names as `nodepool_vm0` and `clientpool_vm0`.
            # IP list output by terraform must be of the form:
            # nodepool_vm0 <private ip address>
            if line.startswith("node"):
                node_cnt += 1
                if max_nodes != -1 and node_cnt > max_nodes:
                    continue
                ip = line.split()[1]
                nodelist["node" + str(node_cnt)] = (ip.strip(), "node" + str(node_cnt) + domain_suffix)
            
            if line.startswith("client"):
                client_cnt += 1

    return (nodelist, client_cnt)

def get_default_node_params(num_nodes, repeats, seconds):
    bench_params = {
        'faults': 0,
        'nodes': [num_nodes],
        'workers': 1,
        'co-locate': True,
        'rate': [240_000],
        'tx_size': 512,
        'duration': seconds,
        'runs': repeats,

        # Unused
        'simulate_partition': True,
        'partition_start': 5,
        'partition_duration': 5,
        'partition_nodes': 1,
    }
    node_params = {
        'timeout_delay': 5_000,  # ms
        'header_size': 32,  # bytes
        'max_header_delay': 5_000,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 5_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 20,  # ms
        'use_optimistic_tips': True,
        'use_parallel_proposals': True,
        'k': 4,
        'use_fast_path': True,
        'fast_path_timeout': 5_000,
        'use_ride_share': False,
        'car_timeout': 5_000,

        'simulate_asynchrony': False,
        'asynchrony_start': 15_000, #ms
        'asynchrony_duration': 3_000, #ms
    }

    return bench_params, node_params


@click.command()
@click.option(
    "-n", "--num_nodes",
    default=4,
    help="Number of nodes",
    type=click.INT
)
@click.option(
    "-ips", "--ip_list",
    default="/dev/null",
    help="File with list of node names and IP addresses to be used with cluster config",
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
def main(num_nodes, ip_list):
    bench_params, node_params = get_default_node_params(num_nodes, 1, 60)
    base_port = 3000
    workers = bench_params['workers']
    node_params = NodeParameters(node_params)

    node_list, _ = gen_cluster_nodelist(ip_list, ".autobahn.org")
    ip_list = [
        v[0] for v in node_list.values()
    ]
    print(ip_list)


    gen_config(num_nodes, base_port, workers, node_params, ip_list)



if __name__ == "__main__":
    main()
