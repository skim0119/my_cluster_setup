worker_init = f"""
# Prepare the computational environment
source ~/localrc_miv.sh

# Print the environment details for debugging
hostname
pwd
which python
"""
