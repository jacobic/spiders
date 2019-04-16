import os

import emcee
import pandas as pd
import numpy as np
import tqdm
import multiprocess as mp


def file_size(path, unit='B'):
    if os.path.isfile(path):
        size = os.path.getsize(path)
    else:
        size = 0
    n = {'B': 0, 'KB': 1, 'MB': 2}
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
    df = pd.Panel(sampler.chain) \
        .to_frame() \
        .stack() \
        .reset_index()
    df.columns = ['step', 'par', 'walker', 'value']
    if nburn:
        # Burn the chain.
        df = df.query(f'step > {nburn}')
    if pars:
        # Map par values with keys.
        df['par'] = df['par'].map(lambda _: pars[_])
    return df



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
