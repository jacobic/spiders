import os
from typing import Iterable
import emcee
import pandas as pd
import numpy as np


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


def mesh_pandas(data: Iterable[np.array], **kw):
    """

    Parameters
    ----------
    data : Iterable[np.array]
        Data to create meshgrid from.
    kw : dict
        Keyword arguments for pd.DataFrame constructor.

    Returns
    -------
    df : pd.DataFrame
        Meshgrid of of `data` in a convenient tabular format.

    """
    data = np.array(np.meshgrid(*data)).reshape(len(data), -1).T
    df = pd.DataFrame(data=data, **kw)
    return df


def emcee_pandas(sampler: emcee.EnsembleSampler, nburn: int = 0,
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
    df : pd.DataFrame
        Burnt in chain

    """

    chain = sampler.get_chain(discard=nburn, flat=False)
    # Convert chain to a pd.DataFrame for easy plotting with sns.
    df = pd.Panel(chain).to_frame().stack().reset_index()
    # TODO: Panel will be deprected soon so need to a replacement with x-array?
    df.columns = ['step', 'par', 'walker', 'value']

    # Sort before potential mapping.
    df = df.sort_values(['par', 'step', 'walker'])

    if pars:
        # Map par values with keys.
        df['par'] = df['par'].map(lambda _: pars[_])

    df = df.set_index('step', drop=False)

    return df


def evalrecurse_dict(dictionary: dict, evaluate: Iterable[str]):
    """
    Evaluate python expressions from strings in a dictionary.

    Parameters
    ----------
    dictionary : dict
    evaluate : str or Iterable[str]
        key or iterable of keys of the dictionary to evaluate.
    """
    if not isinstance(evaluate, (tuple, list, np.array)):
        evaluate = list(evaluate)
    for k, v in dictionary.items():
        if isinstance(v, dict):
            evalrecurse_dict(v, evaluate=evaluate)
        elif k in evaluate and isinstance(v, str):
            dictionary[k] = eval(v)
