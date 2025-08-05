import os

from parsl.config import Config
from parsl.providers import SlurmProvider, LocalProvider
from parsl.executors import HighThroughputExecutor
from parsl.executors.threads import ThreadPoolExecutor
from parsl.launchers import SrunMPILauncher, SrunLauncher, SimpleLauncher, MpiExecLauncher
from parsl.addresses import address_by_hostname

from my_cluster_setup.mpi_hostfile_utils import get_nodelist

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


def local_htex(label="local_htex", max_workers_per_node=56):
    """
    Start reducing to 0 to increase memory per job.
    Essentially, the number of worker is determined by max_workers_per_node * ratio.
    Remaining will be used for thread.
    """
    from .frontera_init import get_worker_init
    from parsl.launchers import SingleNodeLauncher, SrunLauncher

    provider = LocalProvider(
        launcher=SrunLauncher(),
        init_blocks=1,
        max_blocks=1,
        nodes_per_block=os.environ["SLURM_NNODES"],
        worker_init=get_worker_init(),
    )

    config = Config(
        retries=8,
        run_dir=os.path.expandvars("$SCRATCH/runinfo"),
        executors=[
            HighThroughputExecutor(
                label=label,
                # This option sets our 1 manager running on the lead node of the job
                # to spin up enough workers to concurrently invoke `ibrun <mpi_app>` calls
                max_workers_per_node=max_workers_per_node,
                cores_per_worker=1e-6,
                # Set the heartbeat params to avoid faults from periods of network unavailability
                # Addresses network drop concern from older Claire communication
                provider=provider,
            )
        ],
    )
    return config
