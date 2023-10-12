from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import SlurmProvider, LocalProvider
from parsl.executors import HighThroughputExecutor
from parsl.executors.threads import ThreadPoolExecutor
from parsl.launchers import SrunMPILauncher, SrunLauncher, SimpleLauncher, MpiExecLauncher
from parsl.addresses import address_by_hostname


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


def local_htex(label="local_htex"):
    provider = LocalProvider(
        channel=LocalChannel(),
        init_blocks=1,
        max_blocks=1,
    )

    config = Config(
        executors=[
            HighThroughputExecutor(
                label=label,
                # This option sets our 1 manager running on the lead node of the job
                # to spin up enough workers to concurrently invoke `ibrun <mpi_app>` calls
                cores_per_worker=1,
                # Set the heartbeat params to avoid faults from periods of network unavailability
                # Addresses network drop concern from older Claire communication
                heartbeat_period=60,
                heartbeat_threshold=300,

                provider=provider,
            ),
        ],
    )
    return config
