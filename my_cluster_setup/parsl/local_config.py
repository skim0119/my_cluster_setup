import os

from parsl.config import Config
from parsl.channels import LocalChannel, SSHChannel
from parsl.providers import SlurmProvider, LocalProvider, AdHocProvider
from parsl.executors import HighThroughputExecutor
from parsl.executors.threads import ThreadPoolExecutor
from parsl.launchers import SrunMPILauncher, SrunLauncher, SimpleLauncher, MpiExecLauncher
from parsl.addresses import address_by_hostname

from my_cluster_setup.mpi_hostfile_utils import get_nodelist
from .frontera_init import worker_init

def local_threads(label='local_threads', max_threads=None):
    if max_threads is None:
        import multiprocessing as mp
        max_threads = mp.cpu_count()
    elif isinstance(max_threads, float):
        import multiprocessing as mp
        max_threads = int(mp.cpu_count() * max_threads)

    return Config(
        executors=[
            ThreadPoolExecutor(max_threads=max_threads, label=label)
        ]
    )


def local_htex(label="local_htex", work_memory_ratio=1.0, max_workers_per_node=56, n_node=1):
    """
    work_memory_ratio: Set 1 to be closer to cpu-heavy load.
    Start reducing to 0 to increase memory per job.
    Essentially, the number of worker is determined by max_workers_per_node * ratio.
    Remaining will be used for thread.
    """
    provider = LocalProvider(
        channel=LocalChannel(),
        init_blocks=1,
        max_blocks=1,
        nodes_per_block=1,
    )

    max_workers = int(max_workers_per_node * work_memory_ratio)
    cores_per_worker = int(max_workers_per_node) // max_workers

    config = Config(
        retries=8,
        executors=[
            HighThroughputExecutor(
                label=label,
                # This option sets our 1 manager running on the lead node of the job
                # to spin up enough workers to concurrently invoke `ibrun <mpi_app>` calls
                max_workers=max_workers * n_node,
                cores_per_worker=cores_per_worker,
                # Set the heartbeat params to avoid faults from periods of network unavailability
                # Addresses network drop concern from older Claire communication
                provider=provider,
            )
        ],
    )
    return config

def local_htex_ssh(label="local_htex_ssh", work_memory_ratio=1.0, max_workers_per_node=56, n_node=1):
    """
    work_memory_ratio: Set 1 to be closer to cpu-heavy load.
    Start reducing to 0 to increase memory per job.
    Essentially, the number of worker is determined by max_workers_per_node * ratio.
    Remaining will be used for thread.
    """
    # provider = LocalProvider(
    #     channel=LocalChannel(),
    #     init_blocks=1,
    #     max_blocks=1,
    #     nodes_per_block=1,
    # )

    nodes, current_node = get_nodelist()

    #channels = [LocalChannel()]+[SSHChannel(node) for node in nodes if node != current_node]
    #channels = [SSHChannel(node) for node in nodes if node != current_node]
    channels = [SSHChannel(node) for node in nodes]

    provider = AdHocProvider(
        channels=channels,
        worker_init=worker_init,
        move_files=False,  # This is rather bug, but needed
    )

    max_workers = int(max_workers_per_node * work_memory_ratio)
    cores_per_worker = int(max_workers_per_node) // max_workers

    config = Config(
        retries=8,
        executors=[
            HighThroughputExecutor(
                label=label,
                # This option sets our 1 manager running on the lead node of the job
                # to spin up enough workers to concurrently invoke `ibrun <mpi_app>` calls
                max_workers=max_workers,
                cores_per_worker=cores_per_worker,
                # Set the heartbeat params to avoid faults from periods of network unavailability
                # Addresses network drop concern from older Claire communication
                provider=provider,
            )
        ],
        strategy=None,
        run_dir=os.path.expandvars("$SCRATCH/runinfo")
    )
    return config
