import os
import subprocess
import re

import logging


from parsl.launchers.launchers import Launcher, SrunLauncher
from my_cluster_setup.mpi_hostfile_utils import parse_nodelist, get_nodelist, get_runname

logger = logging.getLogger(__name__)


class SrunLauncherV2(SrunLauncher):
    """
    
    """

    def __init__(self, debug: bool = True, overrides: str = '', finalize_cmds: tuple[str]=()):
        """
        Parameters
        ----------

        overrides: str
             This string will be passed to the srun launcher. Default: ''
        """

        super().__init__(debug=debug)
        self.overrides = overrides
        self.final_cmds = "\n".join(finalize_cmds)

    def __call__(self, command: str, tasks_per_node: int, nodes_per_block: int) -> str:
        """
        Args:
        - command (string): The command string to be launched

        """
        # cmd = super().__call__(command, tasks_per_node, nodes_per_block)
        task_blocks = tasks_per_node * nodes_per_block
        debug_num = int(self.debug)

        cmd = '''#set -e
export CORES=$SLURM_CPUS_ON_NODE
export NODES=$SLURM_JOB_NUM_NODES

[[ "{debug}" == "1" ]] && echo "Found cores : $CORES"
[[ "{debug}" == "1" ]] && echo "Found nodes : $NODES"
WORKERCOUNT={task_blocks}

cat << SLURM_EOF > cmd_$SLURM_JOB_NAME.sh
{command}
SLURM_EOF
chmod a+x cmd_$SLURM_JOB_NAME.sh

# srun --ntasks {task_blocks} bash cmd_$SLURM_JOB_NAME.sh || echo "something went wrong during srun"
# srun --ntasks {task_blocks} -l {overrides} bash cmd_$SLURM_JOB_NAME.sh
ibrun bash cmd_$SLURM_JOB_NAME.sh &
wait

'''.format(command=command,
           task_blocks=task_blocks,
           overrides=self.overrides,
           debug=debug_num)

        x = f'''
{cmd}
{self.final_cmds}

[[ "{debug_num}" == "1" ]] && echo "Done"
'''
        return x


class GnuParallelLauncher(Launcher):
    # FIXME
    """ Worker launcher that wraps the user's command with the framework to
    launch multiple command invocations via GNU parallel sshlogin.

    This wrapper sets the bash env variable CORES to the number of cores on the
    machine.

    This launcher makes the following assumptions:

    - GNU parallel is installed and can be located in $PATH
    - Paswordless SSH login is configured between the controller node and the
      target nodes.
    - The provider makes available the $SLURM_NODEFILE environment variable
    """
    def __init__(self, debug: bool = True):
        super().__init__(debug=debug)

    def __call__(self, command: str, tasks_per_node: int, nodes_per_block: int) -> str:
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.
        """
        task_blocks = tasks_per_node * nodes_per_block
        debug_num = int(self.debug)

        job_name = get_runname()
        node_list, host_node = get_nodelist()

        ssh_login_file = f"{job_name}.nodes"
        # create_nodelist_environ()
        logging.debug(f"{node_list}")

        x = '''set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "{debug}" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT={task_blocks}
JOBNAME={job_name}

# Deduplicate the nodefile
SSHLOGINFILE="{ssh_login_file}"
if [ -z "$SLURM_NODEFILE" ]; then
    echo "localhost" > $SSHLOGINFILE
else
    sort -u $SLURM_NODEFILE > $SSHLOGINFILE
fi

cat << PARALLEL_CMD_EOF > cmd_$JOBNAME.sh
{command}
PARALLEL_CMD_EOF
chmod u+x cmd_$JOBNAME.sh

#file to contain the commands to parallel
PFILE=cmd_${{JOBNAME}}.sh.parallel

# Truncate the file
cp /dev/null $PFILE

for COUNT in $(seq 1 1 $WORKERCOUNT)
do
    echo "pwd; sh cmd_$JOBNAME.sh" >> $PFILE
done

parallel --record-env
parallel --env _ --joblog "$JOBNAME.sh.parallel.log" \
            --sshloginfile $SSHLOGINFILE --jobs {tasks_per_node} < $PFILE

[[ "{debug}" == "1" ]] && echo "All workers done"
'''.format(command=command,
        job_name=job_name,
        ssh_login_file=ssh_login_file,
        tasks_per_node=tasks_per_node,
        task_blocks=task_blocks,
        debug=debug_num)
        return x

class SingleNodeLauncher(Launcher):
    """ Worker launcher that wraps the user's command with the framework to
    launch multiple command invocations in parallel. This wrapper sets the
    bash env variable CORES to the number of cores on the machine. By setting
    task_blocks to an integer or to a bash expression the number of invocations
    of the command to be launched can be controlled.
    """
    def __init__(self, debug: bool = True, fail_on_any: bool = False):
        super().__init__(debug=debug)
        self.fail_on_any = fail_on_any

    def __call__(self, command, tasks_per_node, nodes_per_block):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.
        - fail_on_any: If True, return a nonzero exit code if any worker failed, otherwise zero;
                       if False, return a nonzero exit code if all workers failed, otherwise zero.

        """
        task_blocks = tasks_per_node * nodes_per_block
        fail_on_any_num = int(self.fail_on_any)
        debug_num = int(self.debug)

        x = '''set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "{debug}" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT={task_blocks}
FAILONANY={fail_on_any}
PIDS=""

CMD() {{
ibrun -n 1 task_affinity {command}
}}
for COUNT in $(seq 1 1 $WORKERCOUNT); do
    [[ "{debug}" == "1" ]] && echo "Launching worker: $COUNT"
    CMD $COUNT &
    PIDS="$PIDS $!"
done

ALLFAILED=1
ANYFAILED=0
for PID in $PIDS ; do
    wait $PID
    if [ "$?" != "0" ]; then
        ANYFAILED=1
    else
        ALLFAILED=0
    fi
done

[[ "{debug}" == "1" ]] && echo "All workers done"
if [ "$FAILONANY" == "1" ]; then
    exit $ANYFAILED
else
    exit $ALLFAILED
fi
'''.format(command=command,
           task_blocks=task_blocks,
           debug=debug_num,
           fail_on_any=fail_on_any_num)
        return x

if __name__ == "__main__":
    command = f'scontrol show job | grep " NodeList="'
    outputs = subprocess.check_output(command, shell=True).decode("utf-8")
    for output in [output.strip() for output in outputs.split('\n')]:
        nodelist_str = output.split('=')[-1].strip()
        if not nodelist_str:
            continue
        print(nodelist_str)
        print(parse_nodelist(nodelist_str), len(parse_nodelist(nodelist_str)))
