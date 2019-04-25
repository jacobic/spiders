import os
import emcee
import pandas as pd
import numpy as np
from scipy.stats import norm


def file_size(path, unit='B'):
    if os.path.isfile(path):
        size = os.path.getsize(path)
    else:
        size = 0
    return size / pow(1024, dict(B=0, KB=1, MB=2)[unit])


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

