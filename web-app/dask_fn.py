def get_pbs_cluster(MAX_WORKERS):
    """ Create cluster through dask_jobqueue.   
    """
    from dask_jobqueue import PBSCluster
    
    num_jobs = MAX_WORKERS
    walltime = '0:10:00'
    memory = '4GB' 

    cluster = PBSCluster(cores=1, processes=1, walltime=walltime, memory=memory, queue='casper', 
                         resource_spec=f"select=1:ncpus=1:mem={memory}",)
    cluster.scale(jobs=num_jobs)
    return cluster

def get_gateway_cluster(MAX_WORKERS):
    """ Create cluster through dask_gateway
    """
    from dask_gateway import Gateway

    gateway = Gateway()
    cluster = gateway.new_cluster()
    cluster.adapt(minimum=2, maximum=MAX_WORKERS)
    return cluster

def get_local_cluster(MAX_WORKERS):
    """ Create cluster using the Jupyter server's resources
    """
    from distributed import LocalCluster
    cluster = LocalCluster()    

    cluster.scale(MAX_WORKERS)
    return cluster