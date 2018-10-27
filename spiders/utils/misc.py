import os
import tqdm
import multiprocess as mp


def file_size(path, unit='B'):
    if os.path.isfile(path):
        size = os.path.getsize(path)
    else:
        size = 0
    n = {'B': 0, 'KB': 1, 'MB': 2}
    return size / pow(1024, n[unit])

@dataclass
class FileBase:
    """
    Parameters
    ----------
    name : str
        Partial file name, includes extension but excludes path to file.
    """
    name : str
    subdir :str
    abspath : str

    def __post_init__(self):
        self.size = file_size(self.abspath, 'MB')
        self.exists = os.path.isfile(self.abspath)

    def _size(self, unit='MB'):
        return file_size(self.abspath, unit)

class Str(str):
    """
    Subclass that extends the format string for strings to begin
    with an optional 'u' or 'l' to shift to upper or lower case.
    """

    def __format__(self, fmt):
        """
        Parameters
        ----------
        fmt : {'u', 'l'}
            Upper or Lower
        """
        s = str(self)
        if fmt[0] == 'u':
            s = self.upper()
            fmt = fmt[1:]
        elif fmt[0] == 'l':
            s = self.lower()
            fmt = fmt[1:]
        return s.__format__(fmt)

    def __getitem__(self, item):
        """
        Returns
        ----------
        The item as the form of another Str object.
        """
        return Str(str(self).__getitem__(item))


def assert_arraylike(x):
    # Ensure that x is array-like
    if isinstance(x, (int, float)):
        x = [x]
    # Ensure that x is a numpy array as np.where is used in this function.
    if isinstance(x, list):
        x = np.array(x)
    if isinstance(x, pd.Series):
        x = x.values
    assert isinstance(x, np.ndarray)
    return x


def parallelize(func, inputs, n_proc, sync=True, async=False, loglevel=None):
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
