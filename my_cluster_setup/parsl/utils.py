import os
import subprocess
import re

import logging

logger = logging.getLogger(__name__)

def parse_nodelist(nodelist_str):
    pattern_comma = r',(?![^\[]*\])'
    pattern_dash = r'-(?![^\[]*\])'
    node_list = re.split(pattern_comma, nodelist_str)

    nodes = []
    for part in node_list:
        prefix, ranges = re.split(pattern_dash, part)
        for num in ranges.strip('[').strip(']').split(','):
            if '-' in num:
                l, h = num.split('-')
                for n in range(int(l), int(h)+1):
                    nodes.append(f"{prefix}-{n:03d}")
            else:
                nodes.append(f"{prefix}-{num}")
    return nodes


# TODO: Maybe move to general utility
def get_nodelist():
    """
    return list of nodes and current_node
    """
    try:
        jobid = os.environ["SLURM_JOBID"]
        current_node = os.environ["SLURMD_NODENAME"]
    except KeyError:
        return ["localhost"], "localhost"

    command = f'scontrol show job {jobid} | grep " NodeList="'

    output = subprocess.check_output(command, shell=True).decode("utf-8").strip()
    nodelist_str = output.split('=')[-1].strip()

    nodes = parse_nodelist(nodelist_str)
    return nodes, current_node

def create_nodelist_environ():
    nodes, _ = get_nodelist()
    var_name = "SLURM_NODEFILE"
    if not var_name in os.environ:
        os.environ[var_name] = '\n'.join(nodes)

def get_runname():
    try:
        jobid = os.environ["SLURM_JOBID"]
    except KeyError:
        return None
    command = f'scontrol show job {jobid} | grep " JobName="'
    output = subprocess.check_output(command, shell=True).decode("utf-8").strip()
    name = output.split('=')[-1].strip()
    return name

