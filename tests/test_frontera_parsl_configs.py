import pytest

def test_CONFIG_FRONTERA_DEV_SIMPLE_load():
    from my_cluster_setup.parsl import CONFIG_FRONTERA_DEV_SIMPLE
    pass


def test_frontera_mpi_configs():
    from my_cluster_setup.parsl import frontera_mpi_config

    frontera_mpi_config(num_nodes=2, partition="small")
    frontera_mpi_config(num_nodes=4, partition="normal")
    frontera_mpi_config(num_nodes=2, partition="development")
