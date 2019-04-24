import scipy.integrate as inte
from colossus.lss import mass_function as mf
import numpy as np

def int_hmf(m: float, z: float, cosmo, mdef, model, method='romb', **kw) -> float:
    """

    Parameters
    ----------
    m : str, optional
        Mass  into `_` pd.Series.
    z : str, optional
        Redshift  into `_` pd.Series.
    modkey :str, optional
        Model key into `_` pd.Series.
    method : {'simps', 'romb'}, optional
        Integration method, scipy.integrate.<method>.
    kw :dict

    Returns
    -------
    n : float
        Comoving number density of halos greater than the limiting halo mass
        at a given redshift.

    Integrate in linear mass intervals rather than logarithmic to maximise
    accuracy. colossus.lss.mass_function expects units of mass to be in ..
    math:: M_\odot h^{-1} and returns dndlnM in units of

    .. math::
   \begin{eqnarray}
    n(M > M_{lim}, z) & = \int_{M_{lim}}^{\infty} \dfrac{dn}{dlnM} dlnM \\
           & = \int_{M_{lim}}^{\infty} \dfrac{dn}{dlnM} \dfrac{dM}{M} \\
           & \approx \int_{M_{lim}}^{M_{max}} \dfrac{dn}{dlnM} \dfrac{dM}{M}
    \end{eqnarray}
    """

    limits = m, np.power(10, 15.5)  # / u.liitleh
    # kw = dict(q_in='M', q_out='dndlnM', mdef=cm.m['def'], model=cm.model['hmf'])
    kw = dict(q_in='M', q_out='dndlnM', mdef=mdef, model=model)
    cosmo.checkForChangedCosmology()

    # Number of samples must be one plus a non-negative power of 2
    x = np.linspace(*limits, num=2 ** 6 + 1)
    y = mf.massFunction(x=x, z=z, **kw) / x
    # dndMs = e.dndM(x, z)
    n = getattr(inte, method)(y, dx=x[1] - x[0])

    try:
        # Some integration methods also return [result, error].
        return n[0]
    except IndexError:
        return n
