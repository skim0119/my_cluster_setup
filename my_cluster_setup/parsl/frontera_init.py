def get_worker_init(init_source_file="~localrc_miv_analysis.sh"):
    worker_init = f"""
    # Prepare the computational environment
    source {init_source_file}

    # Print the environment details for debugging
    echo "Jobscript configuration with:"
    hostname
    pwd
    which python
    """
    return worker_init
