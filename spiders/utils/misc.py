import os
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
