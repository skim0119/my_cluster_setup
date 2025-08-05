import os
from parsl.config import Config
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor
from parsl.launchers import (
    SrunMPILauncher,
    SrunLauncher,
    SimpleLauncher,
    MpiExecLauncher,
    WrappedLauncher,
)

# from .launchers import SingleNodeLauncher
from parsl.addresses import address_by_hostname, address_by_interface

# from parsl.jobs.error_handlers import windowed_error_handler

from .frontera_init import get_worker_init
from .launchers import SrunLauncherV2


# TODO: add flex partition
def frontera_mpi_config(
    num_nodes=2,
    max_workers_per_node=None,  # Caps the number of workers per node
    ranks_per_node=56,
    partition="normal",
    walltime="48:00:00",
    max_blocks=1,
    over_subscription=False,
    init_source_file=None,
    finalize_cmds: tuple[str] = (),
    scratch_path: str | None = None,
):
    if scratch_path is None:
        scratch_path = os.environ["SCRATCH"]
    # Limiting number of rank used for each nodes
    if max_workers_per_node is None:
        max_workers_per_node = ranks_per_node

    # [development] limit walltime for dev
    if partition == "development":
        walltime = "02:00:00"

    # Partition limits: https://frontera-portal.tacc.utexas.edu/user-guide/running/
    if partition == "normal":
        assert num_nodes >= 3
        assert num_nodes <= 3512
    elif partition == "small":
        assert num_nodes <= 2
    elif partition == "development":
        assert num_nodes <= 40
    assert num_nodes >= 1

    # Decide launcher
    #launcher = SimpleLauncher(debug=False)
    #launcher = SrunLauncher(debug=False)
    launcher = SrunLauncherV2(finalize_cmds=finalize_cmds, debug=False)
    #launcher = SingleNodeLauncher()

    # TODO
    # scheduler_options=f"#SBATCH --ntasks-per-node={max_workers_per_node}"
    scheduler_options=""

   # TODO: need little more explanation of what this is doing
    if over_subscription:
        cores_per_worker = 1e-6
    cores_per_worker = ranks_per_node // max_workers_per_node
    # cores_per_worker = 1

    if init_source_file is None:
        worker_init = get_worker_init()
    else:
        worker_init = get_worker_init(init_source_file=init_source_file)

    config = Config(
        # retries=4,
        internal_tasks_max_threads=8,
        run_dir=os.path.join(scratch_path, "runinfo"),
        executors=[
            # HighThroughputExecutor(
            #     label="HtexExecutorDev",
            #     address=address_by_interface("ib0"),
            #     max_workers_per_node=max_workers_per_node,
            #     cores_per_worker=cores_per_worker,
            #     provider=SlurmProvider(
            #         partition="development",
            #         cmd_timeout=60,
            #         nodes_per_block=40,  # Number of nodes
            #         #cores_per_node=ranks_per_node,
            #         walltime="02:00:00",
            #         # Set scaling limits
            #         init_blocks=1,
            #         min_blocks=1,
            #         max_blocks=1,
            #         # Specify number of ranks
            #         scheduler_options="",
            #         launcher=launcher,
            #         worker_init=worker_init,
            #         # exclusive=True,
            #         # parallelism=1.0,
            #     ),
            # ),
            HighThroughputExecutor(
                label="HtexExecutor",
                #address=address_by_hostname(),
                address=address_by_interface("ib0"),
                # This option sets our 1 manager running on the lead node of the job
                # to spin up enough workers to concurrently invoke `ibrun <mpi_app>` calls
                max_workers_per_node=max_workers_per_node,
                cores_per_worker=cores_per_worker,
                # mem_per_worker=16,
                # Set the heartbeat params to avoid faults from periods of network unavailability
                # Addresses network drop concern from older Claire communication
                # heartbeat_period=120,
                # heartbeat_threshold=910,
                # block_error_handler=windowed_error_handler,
                # cpu_affinity="none", # block", # "none", "alternating", "block-reverse"
                # mpi_launcher="ibrun",
                # encrypted = False,
                provider=SlurmProvider(
                    partition=partition,
                    cmd_timeout=60,
                    nodes_per_block=num_nodes,  # Number of nodes
                    # cores_per_node=ranks_per_node,
                    walltime=walltime,
                    # Set scaling limits
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=max_blocks,
                    # Specify number of ranks
                    scheduler_options=scheduler_options,
                    launcher=launcher,
                    worker_init=worker_init,
                    # exclusive=True,
                    # parallelism=1.0,
                ),
            ),
        ],
    )
    return config

def frontera_mpi_htex_config(
    num_nodes=2,
    ntasks_per_node=56,
    cores_per_worker=4,
    partition="normal",
    walltime="48:00:00",
    max_blocks=1,
    init_source_file=None,
    finalize_cmds: tuple[str]=(),
):
    # Limiting number of rank used for each nodes
    max_workers = int(ranks_per_node * num_nodes / cores_per_worker)

    # [development] limit walltime for dev
    if partition == "development":
        walltime="02:00:00"

    # Partition limits: https://frontera-portal.tacc.utexas.edu/user-guide/running/
    if partition == "normal":
        assert num_nodes >= 3
        assert num_nodes <= 3512
    elif partition == "small":
        assert num_nodes <= 2
    elif partition == "development":
        assert num_nodes <= 40
    assert num_nodes >= 1

    # Decide launcher
    #launcher = SimpleLauncher(debug=False)
    launcher = WrappedLauncher(prepend=f"ibrun -n {cores_per_worker}", debug=False)
    #launcher = SrunLauncher(debug=False)
    #launcher = SrunLauncherV2(finalize_cmds=finalize_cmds, debug=False)
    #launcher = SingleNodeLauncher()

    # TODO
    scheduler_options=f"#SBATCH --ntasks-per-node={ntasks_per_node}"

    if init_source_file is None:
        worker_init = get_worker_init()
    else:
        worker_init = get_worker_init(init_source_file=init_source_file)

    config = Config(
        # retries=4,
        internal_tasks_max_threads=4,
        run_dir=os.path.join(scratch_path, "runinfo"),
        executors=[
            HighThroughputExecutor(
                label="HtexExecutor",
                #address=address_by_hostname(),
                address=address_by_interface("ib0"),
                # This option sets our 1 manager running on the lead node of the job
                # to spin up enough workers to concurrently invoke `ibrun <mpi_app>` calls
                max_workers_per_node=max_workers // num_nodes,
                cores_per_worker=1e-6,
                # mem_per_worker=16,
                # Set the heartbeat params to avoid faults from periods of network unavailability
                # Addresses network drop concern from older Claire communication
                # heartbeat_period=120,
                # heartbeat_threshold=910,

                # block_error_handler=windowed_error_handler,

                # cpu_affinity="none", # block", # "none", "alternating", "block-reverse"
                # mpi_launcher="ibrun",
                # encrypted = False,

                provider=SlurmProvider(
                    partition=partition,
                    cmd_timeout=60,
                    nodes_per_block=num_nodes,  # Number of nodes
                    walltime=walltime,
                    # Set scaling limits
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=max_blocks,
                    # Specify number of ranks
                    scheduler_options=scheduler_options,
                    launcher=launcher,
                    worker_init=worker_init,
                    # exclusive=True,
                    # parallelism=1.0,
                ),
            ),
        ],
    )
    return config

CONFIG_FRONTERA_DEV_SIMPLE = frontera_mpi_config(num_nodes=1, partition="development", walltime="02:00:00")
CONFIG_FRONTERA_DEV_PARALLEL = frontera_mpi_config(num_nodes=4, partition="development", walltime="02:00:00", max_workers_per_node=14)
