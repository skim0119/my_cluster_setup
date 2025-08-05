from parsl.config import Config
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunMPILauncher, SrunLauncher, SimpleLauncher, MpiExecLauncher
from parsl.addresses import address_by_hostname

from .expanse_init import worker_init


def expanse_mpi_config(
    label="expanse_htex",
    num_nodes=2,
    max_ranks_per_node=None,  # Caps the number of workers per node
    ranks_per_node=128,
    partition="compute",
    walltime="48:00:00",
    _blocks=1,
    wrap_ibrun=False,
    mpi=False,
):
    # Limiting number of rank used for each nodes
    if max_ranks_per_node is None:
        max_ranks_per_node = ranks_per_node

    # Partition limits: 
    if partition == "compute":
        assert num_nodes >= 1
        assert num_nodes <= 32 
    elif partition == "shared":
        assert num_nodes == 1
    elif partition == "debug":
        assert num_nodes >= 1
        assert num_nodes <= 2
    else:
        raise ValueError(f"Partition {partition} not supported.")
    assert num_nodes >= 1

    # Decide launcher
    if wrap_ibrun:
        launcher = MpiExecLauncher()
    else:
        launcher = SimpleLauncher()

    # TODO
    if mpi:
        scheduler_options=f"#SBATCH --ntasks-per-node={max_ranks_per_node}"
        cores_per_worker = 1e-6
    else:
        scheduler_options=""
        cores_per_worker = ranks_per_node // max_ranks_per_node


    config = Config(
        executors=[
            HighThroughputExecutor(
                label=label,
                address=address_by_hostname(),
                # This option sets our 1 manager running on the lead node of the job
                # to spin up enough workers to concurrently invoke `ibrun <mpi_app>` calls
                max_workers=max_ranks_per_node * num_nodes,
                cores_per_worker=cores_per_worker,
                # Set the heartbeat params to avoid faults from periods of network unavailability
                # Addresses network drop concern from older Claire communication
                heartbeat_period=60,
                heartbeat_threshold=300,

                provider=SlurmProvider(
                    partition=partition,
                    cmd_timeout=60,
                    nodes_per_block=num_nodes,  # Number of nodes
                    walltime=walltime,
                    # Set scaling limits
                    init_blocks=_blocks,
                    min_blocks=0,
                    max_blocks=_blocks,
                    # Specify number of ranks
                    scheduler_options=scheduler_options,
                    launcher=launcher,
                    worker_init=worker_init,
                    exclusive=False,
                ),
            ),
        ],
    )
    return config
