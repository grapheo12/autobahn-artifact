# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, primaries, workers, faults=0):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers = len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        (
            self.size,
            self.rate,
            self.start,
            misses,
            self.sent_samples,
            self.client_commits,
            self.all_sent_transactions,
        ) = zip(*results)
        self.misses = sum(misses)

        # Collate client-side observations to keep earliest timestamps per transaction.
        self.sample_sends = self._aggregate_transactions(self.sent_samples)
        self.transaction_sends = self._aggregate_transactions(self.all_sent_transactions)
        self.committed_transactions = self._aggregate_transactions(self.client_commits)

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        proposals, commits, self.configs, primary_ips = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, workers_ips = zip(*results)
        self.sizes = {k: v for x in sizes for k, v in x.items() if k in self.commits}

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _aggregate_transactions(self, dicts):
        aggregated = {}
        for d in dicts:
            for k, v in d.items():
                if k not in aggregated or v < aggregated[k]:
                    aggregated[k] = v
        return aggregated

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sending sample transaction (\d+) from client (\d+)', log)
        samples = {}
        for t, s, c in tmp:
            key = (int(s), int(c))
            samples.setdefault(key, self._to_posix(t))

        # Parse regular transaction sends
        tmp = findall(r'\[(.*Z) .* sending regular transaction (\d+) from client (\d+)', log)
        regular_sends = {}
        for t, s, c in tmp:
            key = (int(s), int(c))
            regular_sends.setdefault(key, self._to_posix(t))

        # Combine sample and regular transaction send times
        all_sends = {}
        all_sends.update(samples)
        all_sends.update(regular_sends)

        # Parse transaction commits - extract both client_id and transaction counter
        tmp = findall(r'\[(.*Z) .* Client (\d+) transaction (\d+) committed', log)
        commits = {}
        for t, client_id, tx_counter in tmp:
            key = (int(tx_counter), int(client_id))
            commits.setdefault(key, self._to_posix(t))

        return size, rate, start, misses, samples, commits, all_sends

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        configs = {
            #'timeout_delay': int(
            #    search(r'Timeout delay .* (\d+)', log).group(1)
            #),
            'header_size': int(
                search(r'Header size .* (\d+)', log).group(1)
            ),
            'max_header_delay': int(
                search(r'Max header delay .* (\d+)', log).group(1)
            ),
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return proposals, commits, configs, ip

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        # Extract sample transactions for latency measurement
        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return sizes, ip

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        
        committed_keys = [
            key for key in self.committed_transactions.keys() if key in self.transaction_sends
        ]
        if not committed_keys or duration <= 0:
            return 0, 0, duration

        unique_tx_count = len(committed_keys)
        bytes = unique_tx_count * self.size[0]
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items()]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.commits or not self.committed_transactions:
            return 0, 0, 0

        committed_keys = [
            key for key in self.committed_transactions.keys() if key in self.transaction_sends
        ]
        if not committed_keys:
            return 0, 0, 0

        start = min(self.transaction_sends[key] for key in committed_keys)
        end = max(self.committed_transactions[key] for key in committed_keys)
        duration = end - start
        if duration <= 0:
            return 0, 0, 0

        unique_tx_count = len(committed_keys)
        print('Num unique transactions committed: ', unique_tx_count)
        bytes = unique_tx_count * self.size[0]
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        sample_latency = []
        all_latency = []
        list_latencies = []
        first_start = 0
        
        # Create combined dicts from aggregated data
        all_sent_samples = self.sample_sends
        all_sent_transactions = self.transaction_sends
        all_client_commits = self.committed_transactions

        print('Num sent samples: ', len(all_sent_samples))
        print('Num sent transactions: ', len(all_sent_transactions))
        print('Num client commits: ', len(all_client_commits))

        # Calculate latency for sample transactions (for mean)
        for tx_id, send_time in all_sent_samples.items():
            if tx_id in all_client_commits:
                commit_time = all_client_commits[tx_id]
                tx_latency = commit_time - send_time

                if first_start == 0 or send_time < first_start:
                    first_start = send_time

                sample_latency.append(tx_latency)
                list_latencies.append((send_time - first_start, commit_time - first_start, tx_latency))
            else:
                print('tx_id not in all_client_commits: ', tx_id)

        # Calculate latency for all transactions (for percentiles)
        for tx_id, send_time in all_sent_transactions.items():
            if tx_id in all_client_commits:
                commit_time = all_client_commits[tx_id]
                tx_latency = commit_time - send_time

                all_latency.append(tx_latency)

        print('Num sample latencies: ', len(sample_latency))
        print('Num all latencies: ', len(all_latency))

        list_latencies.sort(key=lambda tup: tup[0])
        with open('latencies.txt', 'w') as f:
            for line in list_latencies:
                f.write(str(line[0]) + ',' + str(line[1]) + ',' + str((line[2])) + '\n')
        
        # Calculate mean and percentiles
        mean_latency = mean(sample_latency) if sample_latency else 0

        if all_latency:
            sorted_latency = sorted(all_latency)
            n = len(sorted_latency)

            # Calculate percentiles
            p50_idx = int(n * 0.50)
            p95_idx = int(n * 0.95)
            p99_idx = int(n * 0.99)
            p999_idx = int(n * 0.999)

            # Handle edge cases for small sample sizes
            p50 = sorted_latency[min(p50_idx, n-1)]
            p95 = sorted_latency[min(p95_idx, n-1)]
            p99 = sorted_latency[min(p99_idx, n-1)]
            p999 = sorted_latency[min(p999_idx, n-1)]

            return {
                'mean': mean_latency,
                'p50': p50,
                'p95': p95,
                'p99': p99,
                'p999': p999
            }
        else:
            return {
                'mean': mean_latency,
                'p50': 0,
                'p95': 0,
                'p99': 0,
                'p999': 0
            }

    def result(self):
        #timeout_delay = self.configs[0]['timeout_delay']
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']

        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        latency_stats = self._end_to_end_latency()
        end_to_end_latency = latency_stats['mean'] * 1_000
        end_to_end_p50 = latency_stats['p50'] * 1_000
        end_to_end_p95 = latency_stats['p95'] * 1_000
        end_to_end_p99 = latency_stats['p99'] * 1_000
        end_to_end_p999 = latency_stats['p999'] * 1_000

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            #f' Timeout delay: {timeout_delay:,} ms\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency (mean): {round(end_to_end_latency):,} ms\n'
            f' End-to-end latency (P50): {round(end_to_end_p50):,} ms\n'
            f' End-to-end latency (P95): {round(end_to_end_p95):,} ms\n'
            f' End-to-end latency (P99): {round(end_to_end_p99):,} ms\n'
            f' End-to-end latency (P99.9): {round(end_to_end_p999):,} ms\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                workers += [f.read()]

        return cls(clients, primaries, workers, faults=faults)
