import os
import dill as pickle
import fnmatch
import logging
import pandas as pd
import numpy as np
from astropy.table import Table


def load_fits(directory, N_Rvir=10.0):
    """

    :return:
    """
    fits = []
    for file in os.listdir(directory):
        #TODO: change back
        if fnmatch.fnmatch(file,
                           '*.clusters.{0}Rvir.test.fits'.format(N_Rvir)):
            fits.append(os.path.join(directory, file))
    # Set index of each dataframe to cluster halo id
    dfs = [Table.read(f).to_pandas() for f in fits]
    df = pd.concat(dfs, ignore_index=True)
    df.set_index(['ihal', 'isub'])
    return df


def load_pkl(directory, N_Rvir=10.0):
    """

    :param N_Rvir:
    :return:
    """
    try:
        # raise FileNotFoundError
        name = "projected.clusters.{0}Rvir.pkl".format(N_Rvir)
        pkl_path = os.path.join(directory, name)
        data_in = open(pkl_path, "rb")
        print('loading from pkl')
        return pickle.load(data_in)
    except FileNotFoundError as e:
        print('loading from fits')
        df = load_fits(N_Rvir=N_Rvir)

        # For some reason fits files cause bytes in str columns
        # 'O' is for objects, bytes are stored as objects rather than bytes
        # for another weird reason.
        # Convert them all back to strings
        str_df = df.select_dtypes(['O'])
        str_df = str_df.stack().str.decode('utf-8').unstack()
        for col in str_df:
            df[col] = str_df[col]

        # Pickle cleaned up results
        data_out = open(pkl_path, "wb")
        pickle.dump(df, data_out)
        data_out.close()
        return df


def table_to_dict(table):
    """Convert Astropy Table to Python dict.

    Numpy arrays are converted to lists. This Can work with multi-dimensional
    array columns, by representing them as list of list.

    e.g. This is useful in the following situation.

        foo = Table.read('foo.fits')
        foo.to_pandas() <- This will not work if columns are multi-dimensional.

        The alternative is:

        foo = Table.read('foo.fits')
        bar = table_to_dict(foo)
        df = pd.DataFrame(bar, columns=bar.keys()) <- The desired result.
    """
    total_data = {}
    multi_cols = []
    for i, _ in enumerate(table.columns):
        # This looks unusual, but it is the only way to iterate over columns.
        col = table.columns[i]
        data = table[col.name].tolist()
        total_data[col.name] = data
        if len(col.shape) == 2:
            multi_cols.append(col.name)
    return total_data, multi_cols


def fits_pandas(directory,
                name,
                drop_duplicates=False,
                duplicate_id=None,
                multi_startswith=None,
                missing=[-99999, -1.0]):
    """
    Astropy.Table is not able to convert straight to DataFrame due to
    multivalued elements of certain columns so must be converted to a
    dictionary first then to a DataFrame. Specifying the columns
    explicitly with pd.DataFrame() conserves the order of the columns
    rather than using pd.DataFrame.from_dict() where it is not possible.

    Parameters
    ----------
    directory : str
        Directory where fits file is located.
    name : str
        Name of fits file including extension.
    drop_duplicates : bool, optional
        True if you want to drop duplicate rows from the resulting DataFrame.
    duplicate_id : str, optional
        Column name on which to match (requires drop_duplicates=True).

    Returns
    -------
    df : pd.DataFrame
        DataFrame loaded from fits.
    """
    cat = os.path.join(directory, name)
    dict_table, multi_cols = table_to_dict(Table.read(cat))
    df = pd.DataFrame(dict_table, columns=dict_table.keys())

    if drop_duplicates is True:
        df = df.drop_duplicates(
            subset=duplicate_id, keep='first', inplace=False)

    # For some reason fits files cause bytes in str columns
    # 'O' is for objects, bytes are stored as 'objects' rather than bytes.

    # DataFrame columns whose elements contain lists are also dtype
    # 'object' so to avoid converting them to NaN they must be dropped prior
    #  to converting 'object' dtype columns to strings. Such columns to
    # avoid usually start with ALL_*.

    # Convert byte columns to strings. Avoid multi_cols
    if multi_cols == [] and multi_startswith is not None:
        multi_cols = df.columns.str.startswith(multi_startswith)
        str_df = df.loc[:, ~multi_cols].select_dtypes([np.object])

    elif isinstance(multi_cols, list):
        str_df = df.drop(multi_cols, axis=1).select_dtypes([np.object])

    else:
        str_df = df.select_dtypes([np.object])

    if not str_df.empty:
        # Strip whitespace from either side of strings for each string column
        str_df = str_df.stack().str.decode('utf-8').unstack()
        for col in str_df:
            df[col] = str_df[col].str.strip()

    # Remove missing data
    all_df = df.loc[:, multi_cols]
    for col in all_df:
        df[col] = all_df[col].apply(
            lambda y: [x for x in y if x not in missing])

    return df


#TODO: also include multi cols fnctionality (currently in make-row!)
def pandas_fits(df, drop=None):
    """
    drop all objects manually
    # columns containing elements which contain lists in also must be dropped
    #  if not converted properly.
    # bools must be converted to ints (can be np.16)
    # Strings must be converted to bytes in ordder to write to fits
    Parameters
    ----------
    df

    Returns
    -------

    """
    if drop:
        df = df.drop(drop, axis=1, errors='ignore')

    bool_df = df.select_dtypes('bool')
    for col in bool_df:
        df[col] = bool_df[col].astype(np.int16)

    object_df = df.select_dtypes('O')
    for col in object_df:
        #TODO: optimise this logic to automatically remove other objects.
        if all(df[col].apply(type) == str):
            df.loc[:, col] = object_df.loc[:, col].apply(lambda x: x.encode('utf-8'))

    return Table.from_pandas(df.copy())


def fits_pkl(directory, name, **kwargs):
    """ Load / pickle data with fits_pandas for speedy loading.

    Parameters
    ----------
    directory : str
        Directory where fits file is located.
    name : str
        Name of fits file including extension.
    drop_duplicates : bool, optional
        True if you want to drop duplicate rows from the resulting DataFrame.
    duplicate_id : str, optional
        Column name on which to match (requires drop_duplicates=True).

    Returns
    -------
    df : pd.DataFrame
        DataFrame loaded from fits (or pkl).
    """
    pkl_name = name.replace('fits', 'pkl')
    try:
        # raise FileNotFoundError
        pkl_path = os.path.join(directory, pkl_name)
        data_in = open(pkl_path, "rb")
        logging.info(f'Loading {pkl_path} from pkl')
        return pickle.load(data_in)

    except FileNotFoundError as e:
        logging.info(f'Loading {pkl_path} from fits -> pkl DataFrame')
        df = fits_pandas(directory, name, **kwargs)
        # Pickle cleaned up results for quicker loading next time.
        data_out = open(pkl_path, "wb")
        pickle.dump(df, data_out)
        data_out.close()
        return df


