import execnet
import tqdm
import multiprocess as mp
# multiprocess (pathos fork of multiprocessing) uses dill rather than pickle.
# This allows objects with loggers as attributes to be processed in parallel.

class Str(str):
    """
    Subclass that extends the format string for strings to begin
    with an optional 'u' or 'l' to shift to upper or lower case.
    """

    def __format__(self, fmt):
        """

        Parameters
        ----------
            fmt :

        Returns
        ----------

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

        Parameters
        ----------
            item :

        Returns
        ---------- The item as the form of another Str object.

        """
        return Str(str(self).__getitem__(item))


#TODO: deprecate, use iterrtool.chain instead
def flat(l):
    """

    Parameters
    ----------
    l : list of list

    Returns
    -------
    Flattened list

    """
    return [item for sublist in l for item in sublist]

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


def call_python(module, path, func="main", chdir=None, args=[]):
    """

    Parameters
    ----------
        chdir :
        module :
        func :
        args :
        path :

    Returns
    ----------

    """
    # Don't write bytecode prevents .pyc files from being created/kept.
    spec = "popen//python={0}//dont_write_bytecode".format(path)
    if chdir is not None:
        spec = spec.join("//chdir={0}".format(chdir))
    gw = execnet.makegateway(spec)
    # This option is extremley important. The default configuration of string
    # coercion (2str to 3str, 3str to 2unicode) is inconvient.
    gw.reconfigure(py3str_as_py2str=True)
    channel = gw.remote_exec("""
        from {0} import {1} as the_function
        channel.send(the_function(*channel.receive()))
    """.format(module, func))
    channel.send(args)
    return channel.receive()


def parallelize(func, inputs, n_proc, sync=True, async=False, loglevel=None):
    """Helper function to parrallelise any mappable function either
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

    # TODO: clean this up.  # if sync is True:  #     p.map(func, inputs)  #
    #  inform the processor that no new tasks will be added the pool  #  #
    # pool.close()  #     # wait for map to be completed before proceeding  #
    #      pool.join()  # elif async is True:  #     pool.map_async(func,
    #  inputs)  # , callback=on_result)  #     # inform the processor that no
    #  new tasks will be added the pool  #     pool.close()  #     # wait for
    #  map to be completed before proceeding  #     pool.join()


def file_size(path, unit='B'):
    if os.path.isfile(path):
        size = os.path.getsize(path)
    else:
        size = 0
    n = {'B': 0, 'KB': 1, 'MB': 2}
    return size / pow(1024, n[unit])

