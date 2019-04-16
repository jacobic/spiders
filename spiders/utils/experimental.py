#TODO: in future extend pandas to deal with quantities.
# m = pd.DataFrame(QuantityArray(_m * self.m_unit), dtype=QuantityDtype)
# _lgm = pd.DataFrame(QuantityArray(np.log10(_m.values) * u.Dex(
#     self.m_unit)),
#                     dtype=QuantityDtype)
# _m.columns = self.ks.m

import astropy.units as u
import numpy as np
from pandas.core.arrays import PandasArray, PandasDtype

class QuantityDtype(ExtensionDtype):
    """
    A Pandas ExtensionDtype for NumPy dtypes.
    .. versionadded:: 0.24.0
    This is mostly for internal compatibility, and is not especially
    useful on its own.
    Parameters
    ----------
    dtype : numpy.dtype
    """
    _metadata = ('_dtype',)

    def __init__(self, dtype):
        dtype = np.dtype(dtype)
        self._dtype = dtype
        self._name = dtype.name
        self._type = dtype.type

    def __repr__(self):
        return "QuantityDtype({!r})".format(self.name)

class QuantityArray(PandasArray):
    """
    A pandas ExtensionArray for NumPy data.
    .. versionadded :: 0.24.0
    This is mostly for internal compatibility, and is not especially
    useful on its own.
    Parameters
    ----------
    values : ndarray
        The NumPy ndarray to wrap. Must be 1-dimensional.
    copy : bool, default False
        Whether to copy `values`.
    """
    # If you're wondering why pd.Series(cls) doesn't put the array in an
    # ExtensionBlock, search for `ABCPandasArray`. We check for
    # that _typ to ensure that that users don't unnecessarily use EAs inside
    # pandas internals, which turns off things like block consolidation.
    _typ = "npy_extension"
    __array_priority__ = 1000

    # ------------------------------------------------------------------------
    # Constructors

    def __init__(self, values, copy=False):
        if isinstance(values, type(self)):
            values = u.Quantity(values._ndarray)
        if not isinstance(values, u.Quantity):
            raise ValueError("'values' must be a Quantity array.")

        if values.ndim != 1:
            raise ValueError("PandasArray must be 1-dimensional.")

        if copy:
            values = values.copy()

        self._ndarray = values
        self._dtype = QuantityDtype(values.dtype)

