import os
import sys
import click

from my_cluster_setup.mpi_hostfile_utils import get_nodelist

@click.command()
@click.option('-p', '--path', type=str, help="host mpi path")
@click.option('-c', '--core', type=int, help="number of core for each nodes")
def main(path, core):
    nodes, _ = get_nodelist()

    s = []
    for node in nodes:
        s.append(f"{node}:{core}")
    with open(path, "w") as text_file:
        text_file.write('\n'.join(s))
