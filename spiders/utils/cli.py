import click
from dask_jobqueue import SLURMCluster
from dask.distributed import Client


@click.group()
@click.option('--workers', '-W', type=int, default=8)
@click.option('--nodes_per_worker', '-N', type=int, default=1)
@click.option('--cores', '-n', type=int, default=28)
@click.option('--time', '-t', type=str, default='00:30:00')
@click.option('--partition', '-p', default='express')
@click.pass_context
def dask_hpc(ctx: click.Context, workers, nodes_per_worker: int, cores: int, time: str, partition: str):
    """
    See dask and srun/sbatch documentation for details.

    Parameters
    ----------
    ctx : click Context

    """
    # TODO: add memory stuff
    cluster = SLURMCluster(cores=cores, memory="120GB", queue=partition,
                           job_extra=[f"-N {nodes_per_worker}"], walltime=time,
                           interface='ib0')
    cluster.scale(workers)
    ctx.obj = Client(cluster)
