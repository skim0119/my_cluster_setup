from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunMPILauncher
from parsl.addresses import address_by_hostname

dev_config = Config(
    executors=[
        HighThroughputExecutor(
            label="frontera_htex",
            address=address_by_hostname(),
            max_workers=112,
            provider=SlurmProvider(
                partition='development',
                channel=LocalChannel(),
                nodes_per_block=2,
                cores_per_node=56,
                walltime="02:00:00",
                init_blocks=1,
                launcher=SrunMPILauncher(),
            ),
        )
    ],
)
