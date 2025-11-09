# Copyright(C) Facebook, Inc. and its affiliates.
import subprocess
from math import ceil, floor
from os.path import basename, splitext
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import Key, LocalCommittee, NodeParameters, BenchParameters, ConfigError, TSSKey
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker
from benchmark.settings import Settings


CLIENT_SHUTDOWN_GRACE = 12


class LocalBench:
    BASE_PORT = 2000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
            self.settings = Settings.load('settings.json')
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} > {log_file} 2>&1'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()
        
        try:
            Print.info('Setting up testbed...')
            nodes, rate = self.nodes[0], self.rate[0]

            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            # Generate threshold signature files (skip for autobahn-blips-client-timeouts branch).
            names = [x.name for x in keys]
            if self.settings.branch != 'autobahn-blips-client-timeouts':
                cmd = './node threshold_keys'
                for i in range(nodes):
                    cmd += ' --filename ' + PathMaker.threshold_key_file(i)
                # print(cmd)
                cmd = cmd.split()
                subprocess.run(cmd, capture_output=True, check=True)

                tss_keys = []
                for i in range(nodes):
                    tss_keys += [TSSKey.from_file(PathMaker.threshold_key_file(i))]
                ids = [x.id for x in tss_keys]
            else:
                # For autobahn-blips-client-timeouts branch, use sequential IDs based on key index
                ids = list(range(nodes))
            #print('num workers', self.workers)
            committee = LocalCommittee(names, ids, self.BASE_PORT, self.workers)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())

            # Run the primaries (except the faulty ones).
            for i, address in enumerate(committee.primary_addresses(self.faults)):
                cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.threshold_key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i),
                    PathMaker.parameters_file(),
                    debug=debug,
                    branch=self.settings.branch
                )
                log_file = PathMaker.primary_log_file(i)
                print(cmd)
                self._background_run(cmd, log_file)

            # Run the workers (except the faulty ones).
            workers_addresses = committee.workers_addresses(self.faults)
            for i, addresses in enumerate(workers_addresses):
                for (id, address) in addresses:
                    cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.threshold_key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, id),
                        PathMaker.parameters_file(),
                        id,  # The worker's id.
                        debug=debug,
                        branch=self.settings.branch
                    )
                    log_file = PathMaker.worker_log_file(i, id)
                    self._background_run(cmd, log_file)

            # Wait for workers to be ready before starting clients
            Print.info('Waiting for workers to start...')
            sleep(2)

            # Run the clients (after workers are ready).
            client_addresses = committee.client_addresses()
            client_ack_addresses = committee.client_ack_addresses()
            rate_share = ceil(rate / committee.workers())
            f = floor((nodes - 1) / 3)
            Print.info(f"f: {f}")
            client_id = 0
            for i, addresses in enumerate(workers_addresses):
                for (id, address) in addresses:
                    # Get the corresponding client reply and ack addresses
                    if client_id < len(client_addresses):
                        reply_addr = client_addresses[client_id]
                        ack_addr = client_ack_addresses[client_id]
                        cmd = CommandMaker.run_client(
                            client_id,
                            reply_addr,
                            ack_addr,
                            PathMaker.committee_file(),
                            PathMaker.key_file(i),
                            PathMaker.threshold_key_file(i),
                            f'.db-client-{client_id}',
                            self.tx_size,
                            rate_share,
                            self.worker_fault_tolerance,
                            threshold=f+1,
                            duration=self.duration,
                            branch=self.settings.branch,
                            metrics_file=PathMaker.client_metrics_file(i, id),
                            transaction_timeout=self.transaction_timeout
                        )
                        log_file = PathMaker.client_log_file(i, id)
                        print(f"Client {client_id} command: {cmd}")
                        self._background_run(cmd, log_file)
                    client_id += 1

            # Wait for all transactions to be processed.
            total_run = self.duration + CLIENT_SHUTDOWN_GRACE
            Print.info(f'Running benchmark ({self.duration} sec + {CLIENT_SHUTDOWN_GRACE} sec drain)...')
            sleep(total_run)
            self._kill_nodes()

            # Wait for nodes to gracefully shutdown and flush metrics to disk
            Print.info('Waiting for nodes to shutdown...')
            sleep(1)

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return LogParser.process(
                PathMaker.logs_path(),
                faults=self.faults,
                warmup_seconds=self.latency_warmup,
                cooldown_seconds=self.latency_cooldown,
            )

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)
