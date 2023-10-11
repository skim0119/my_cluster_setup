from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunMPILauncher, SrunLauncher, SimpleLauncher, MpiExecLauncher
from parsl.addresses import address_by_hostname

from .bridges2_init import worker_init


def bridges2_mpi_config(
    label="bridges2_htex",
    num_nodes=2,
    max_ranks_per_node=None,  # Caps the number of workers per node
    ranks_per_node=128,
    partition="RM",
    walltime="48:00:00",
    _blocks=1,
    wrap_ibrun=False,
    mpi=False,
):
    # Limiting number of rank used for each nodes
    if max_ranks_per_node is None:
        max_ranks_per_node = ranks_per_node

    # Partition limits: 
    if partition == "RM":
        assert num_nodes >= 1
        assert num_nodes <= 64
    elif partition == "RM-shared":
        assert num_nodes == 1
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
                    channel=LocalChannel(),
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

# CONFIG_BRIDGES2_DEV_SIMPLE = bridges2_mpi_config(num_nodes=1, partition="development", walltime="02:00:00")
def bridges2_mpi_multinode_config():
    config = Config(
        executors=[
            HighThroughputExecutor(
                label='Bridges_HTEX_multinode',
                max_workers=1,
                provider=SlurmProvider(
                    'YOUR_PARTITION_NAME',  # Specify Partition / QOS, for eg. RM-small
                    nodes_per_block=2,
                    init_blocks=1,
                    # string to prepend to #SBATCH blocks in the submit
                    # script to the scheduler eg: '#SBATCH --gres=gpu:type:n'
                    scheduler_options='',

                    # Command to be run before starting a worker, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    worker_init='',

                    # We request all hyperthreads on a node.
                    launcher=SrunLauncher(),
                    walltime='00:10:00',
                    # Slurm scheduler on Cori can be slow at times,
                    # increase the command timeouts
                    cmd_timeout=120,
                ),
            )
        ]
    )
    return config
