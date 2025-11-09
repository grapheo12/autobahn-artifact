# Copyright(C) Facebook, Inc. and its affiliates.
from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        return (
            f'rm -r .db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node generate_keys --filename {filename}'

    @staticmethod
    def run_primary(keys, threshold_keys, committee, store, parameters, debug=False, branch=None):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        threshold_keys_flag = '' if branch == 'autobahn-blips-client-timeouts' else f'--threshold_keys {threshold_keys} '
        return (f'./node {v} run --keys {keys} {threshold_keys_flag}--committee {committee} '
                f'--store {store} --parameters {parameters} primary')

    @staticmethod
    def run_worker(keys, threshold_keys, committee, store, parameters, id, debug=False, branch=None):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        threshold_keys_flag = '' if branch == 'autobahn-blips-client-timeouts' else f'--threshold_keys {threshold_keys} '
        return (f'./node {v} run --keys {keys} {threshold_keys_flag}--committee {committee} '
                f'--store {store} --parameters {parameters} worker --id {id}')

    @staticmethod
    def run_client(client_id, reply_addr, ack_addr, committee, keys, threshold_keys, store, size, rate, workers, threshold=1, duration=None, branch=None, metrics_file=None, transaction_timeout=150):
        assert isinstance(client_id, int) and 0 <= client_id <= 255
        assert isinstance(reply_addr, str)
        assert isinstance(ack_addr, str)
        assert isinstance(committee, str)
        assert isinstance(keys, str)
        assert isinstance(threshold_keys, str)
        assert isinstance(store, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(workers, int) and workers > 0
        assert isinstance(threshold, int) and threshold > 0
        assert isinstance(transaction_timeout, int) and transaction_timeout > 0
        if duration is not None:
            assert isinstance(duration, int) and duration >= 0
        if metrics_file is not None:
            assert isinstance(metrics_file, str)
        threshold_keys_flag = '' if branch == 'autobahn-blips-client-timeouts' else f'--threshold_keys {threshold_keys} '
        metrics_flag = f' --metrics-file {metrics_file}' if metrics_file else ''
        duration_flag = f' --duration {duration}' if duration is not None else ''
        return f'./node -vvv run --keys {keys} {threshold_keys_flag}--committee {committee} --store {store} client --client-id {client_id} --reply-addr {reply_addr} --ack-addr {ack_addr} --size {size} --rate {rate} --workers {workers} --threshold {threshold} --transaction-timeout {transaction_timeout}{duration_flag}{metrics_flag}'

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'benchmark_client')
        return f'rm node ; rm benchmark_client ; ln -s {node} . ; ln -s {client} .'
