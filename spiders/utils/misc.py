import os

import click
import emcee
import pandas as pd
import numpy as np
import tqdm
import multiprocess as mp
from dask_jobqueue import SLURMCluster
from dask.distributed import Client
from scipy.stats import norm


def file_size(path, unit='B'):
    if os.path.isfile(path):
        size = os.path.getsize(path)
    else:
        size = 0
    n = {
        'B': 0,
        'KB': 1,
        'MB': 2}
    return size / pow(1024, n[unit])


def assert_arraylike(x):
    """
    Ensure that x is array-like
    """
    if isinstance(x, (int, float)):
        x = [x]
    # Ensure that x is a numpy array as np.where is used in this function.
    if isinstance(x, list):
        x = np.array(x)
    if isinstance(x, pd.Series):
        x = x.values
    assert isinstance(x, np.ndarray)
    return x


def emcee_pandas(sampler: emcee.EnsembleSampler, nburn: int = None,
                 pars: list = None):
    """
    Convert emcee chain to pd.DataFrame
    Parameters
    ----------
    sampler : emcee.EnsembleSampler
    nburn : int, optional
        Number of steps to burn in.
    pars : iterable, optional
        List of parameters that when indexed returns the label for that
        parameter. e.g. pars[0] = 'Omega_m'.

    Returns
    -------
    pd.DataFrame

    """
    # Convert chain to a pd.DataFrame for easy plotting with sns.
    df = pd.Panel(sampler.chain).to_frame().stack().reset_index()
    #TODO: Panel will be deprected soon so need to a replacement with x-array?
    df.columns = ['step', 'par', 'walker', 'value']

    # Sort before potential mapping.
    df = df.sort_values(['par', 'step', 'walker'])

    if nburn:
        # Burn the chain.
        df = df.query(f'step > {nburn}')
    if pars:
        # Map par values with keys.
        df['par'] = df['par'].map(lambda _: pars[_])

    df = df.set_index('step', drop=False)

    return df


def pj(bin_edges: tuple, loc: float = 0, scale: float = 1):
    if isinstance(bin_edges, pd.Interval):
        # TODO: account for if bin edges is series of pd.interval
        right, left = bin_edges.left, bin_edges.right
    else:
        right, left = bin_edges

    kw = dict(loc=loc, scale=scale)
    p = norm.cdf(right, **kw) - norm.cdf(left, **kw)
    return p


def bin_uncertainity(bin_edges, observation, uncertainty):
    _pj = pj(bin_edges=bin_edges, loc=observation, scale=uncertainty)
    return np.sum(_pj * (1 - _pj))


def foo():
    df.groupby('bin').agg(
        np.sum(lambda _: pj(_['bin'], _['obs'], _['obs_err'])))


def parallelize(func, inputs, n_proc, loglevel=None):
    """
    Helper function to parrallelise any mappable function either
    synchously or asynchrously using python's multiprocessing module.

    Parameters
    ----------
        func :  Any mappable function.
        inputs :  The arguments to multiproces.
        n_proc :  The maximum number of python processes (engines) that
        can be running at any one time.
        sync (bool): Set to true if synchonous processing is required.
        async (bool): Set to true if asynchonous processing is required.

    Returns
    ----------
        None

    """
    if loglevel is not None:
        logger = mp.log_to_stderr()
        logger.setLevel(loglevel)

    # TODO: could improve by only having one sync/ a sync arg
    with mp.Pool(processes=n_proc) as p:
        # Run multiprocessing with progress bar.
        # TODO: other bands can have (quicker) async map as order no longer
        # matters.
        r = list(tqdm.tqdm(p.imap(func, inputs), total=len(inputs)))
        # inform the processor that no new tasks will be added the pool
        p.close()
        # wait for map to be completed before proceeding
        p.join()


@click.group()
@click.option('--nodes', '-N', type=int, default=8)
@click.option('--cores', '-n', type=int, default=28)
@click.option('--time', '-t', type=str, default='00:30:00')
@click.option('--partition', '-p', default='express')
@click.pass_context
def dask_hpc(ctx: click.Context, nodes: int, cores: int, time: str,
             partition: str):
    """
    See dask and srun/sbatch documentation for details.
    
    Parameters
    ----------
    ctx : click Context

    """
    # TODO: add memory stuff
    cluster = SLURMCluster(cores=cores, memory="120GB", queue=partition,
                           walltime=time, interface='ib0')
    cluster.scale(nodes)
    ctx.obj = Client(cluster)
