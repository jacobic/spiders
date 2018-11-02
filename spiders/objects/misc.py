import os
import shutil
import pandas as pd
from dataclasses import dataclass
from astropy.coordinates import SkyCoord
import subprocess as sp
from ..utils.misc import file_size


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


class WrapperBase:
    """
    # Similar to https://github.com/megalut/sewpy/blob/master/sewpy/sewpy.py
    Parameters
    ----------
    cmd
    logbase
    Returns
    -------
    """

    def __init__(self, cmd, logname):
        self.cmd = list(map(str, cmd))
        self.exectable = cmd[0]
        self.logname = logname

    @property
    def which(self):
        which = shutil.which(self.exectable)
        assert which is not None, f"Cannot find {self.exectable}."
        return which

    def init_log(self):
        """
        Log to file directly as sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
        blocks the writer when the OS buffer gets filled and bad things happen.
        Returns
        -------
        """
        # Write cmd to the log file.
        with open(self.logname, 'w') as log:
            log.write(f'which: {self.which}\n')
            log.write(f'cmd: {" ".join(self.cmd)}\n')
            log.write('\n\n------- stdout / stderr -------\n')

    def run_cmd(self):
        self.init_log()
        # Append output of subprocess to the same log file.
        with open(self.logname, 'a') as log:
            p = sp.Popen(self.cmd, stdout=log, stderr=log)
            p.wait()

@dataclass
class FileBase:
    """
    Parameters
    ----------
    name : str
        Partial file name, includes extension but excludes path to file.
    """
    name: str
    subdir: str
    abspath: str

    def _size(self, unit='MB'):
        return file_size(self.abspath, unit)

    @property
    def size(self):
        return file_size(self.abspath, 'MB')

    @property
    def exists(self):
        return os.path.isfile(self.abspath)


@dataclass
class CandidateBase:
    """
    Galaxy cluster candidate object.

    Parameters
    ----------
    guid : str
        Spiders ID of the Candidate.
    """
    guid: str

    def __str__(self):
        return str(self.guid)


@dataclass
class CandidateWithSky(CandidateBase):
    """
    sky : SkyCoord
        Declination of Candidate centre as inferred from optical data.
    _sky : SkyCoord
        Right ascension of Candidate centre as inferred from X-ray data.
    z : float
        Redshift of Candidate.
    """
    sky: SkyCoord
    _sky: SkyCoord
    z: float


@dataclass
class CandidateWithData(CandidateWithSky):
    _pd: pd.Series


@dataclass
class CandidateWithPhoto(CandidateWithData):
    """
    bands : str
        A concatenation of characters that each represent
        a photometric band relevant to the Candidate. Data should exist
        in each Candidate root directory for each band in bands. Default
            value is expected to be 'igrz'.
    """
    bands: str
