import os
import pandas as pd
from dataclasses import dataclass
from astropy.coordinates import SkyCoord
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

    def __post_init__(self):
        self.size = file_size(self.abspath, 'MB')
        self.exists = os.path.isfile(self.abspath)

    def _size(self, unit='MB'):
        return file_size(self.abspath, unit)


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
