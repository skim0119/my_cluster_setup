from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunMPILauncher, SrunLauncher, SimpleLauncher
from parsl.addresses import address_by_hostname

from .frontera_init import worker_init


# TODO: add flex partition
def frontera_mpi_config(
    label="frontera_htex",
    num_nodes=2,
    max_ranks_per_node=None,  # Caps the number of workers per node
    ranks_per_node=56,
    partition="normal",
    walltime="48:00:00",
):
    if max_ranks_per_node is None:
        max_ranks_per_node = ranks_per_node

    # Partition limits: https://frontera-portal.tacc.utexas.edu/user-guide/running/
    if partition == "normal":
        assert num_nodes >= 3
        assert num_nodes <= 3512
    elif partition == "small":
        assert num_nodes <= 2
    elif partition == "development":
        assert num_nodes <= 40
    assert num_nodes >= 1

    config = Config(
        executors=[
            HighThroughputExecutor(
                label=label,
                address=address_by_hostname(),
                # This option sets our 1 manager running on the lead node of the job
                # to spin up enough workers to concurrently invoke `ibrun <mpi_app>` calls
                max_workers=max_ranks_per_node,
                cores_per_worker=1e-6,
                # Set the heartbeat params to avoid faults from periods of network unavailability
                # Addresses network drop concern from older Claire communication
                heartbeat_period=60,
                heartbeat_threshold=300,

                provider=SlurmProvider(
                    partition=partition,
                    channel=LocalChannel(),
                    cmd_timeout=60,
                    nodes_per_block=num_nodes,  # Number of nodes
                    walltime=walltime,
                    # Set scaling limits
                    init_blocks=1,
                    min_blocks=0,
                    max_blocks=1,
                    # Specify number of ranks
                    scheduler_options=f"#SBATCH --ntasks-per-node={max_ranks_per_node}",
                    launcher=SimpleLauncher(),
                    worker_init=worker_init,
                    exclusive=False,
                ),
            )
        ],
    )
    return config

CONFIG_FRONTERA_DEV_SIMPLE = frontera_mpi_config(num_nodes=1, partition="development", walltime="02:00:00")
