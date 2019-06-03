import numpy as np
import pandas as pd
import astropy.wcs as wcs
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.cosmology.core import Cosmology as AstropyCosmology
from astropy.units import Quantity
from colossus.cosmology.cosmology import Cosmology as ColossusCosmology

from spiders.objects.misc import HybridFlatLambdaCDM


@pd.api.extensions.register_dataframe_accessor("_")
@pd.api.extensions.register_series_accessor("_")
class PandasSkyAccessor(object):
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def sky(self, dict_sky=None, sky_kwargs=None):
        if not dict_sky:
            dict_sky = dict(ra='ra', dec='dec')
        if not sky_kwargs:
            sky_kwargs = dict(unit='deg', frame='icrs')
        ra, dec = self._obj[dict_sky['ra']], self._obj[dict_sky['dec']]
        return SkyCoord(ra=ra, dec=dec, **sky_kwargs)


def dist_circle(nx, ny, x_cen, y_cen):
    """

    Create a 2D numpy array where each value is its distance to a given
    center. "

    Parameters
    ----------
        nx :
        ny :
        x_cen :
        y_cen :

    Returns
    ----------
        mask:

   """
    mask = np.zeros([nx, ny])
    x = np.linspace(0, nx - 1, nx)
    y = np.linspace(0, ny - 1, ny)
    x, y = np.meshgrid(x, y)
    x, y = x.transpose(), y.transpose()
    x_cen, y_cen = y_cen, x_cen  # transpose centers well
    mask = np.sqrt((x - x_cen) ** 2 + (y - y_cen) ** 2)
    return mask


def sky_comoving_volume(cosmo: AstropyCosmology, z1: float, z0: float =0,
                        area : Quantity =41253*u.deg**2):
# def sky_comoving_volume(cosmo: AstropyCosmology, area : Quantity, z1: float,
#                         z0: float =0):
    """
    Helper function to calculate the volume of a redshift bin over a
    specified area. Based on https://rdrr.io/cran/celestial/src/R/cosvol.R

    Parameters
    ----------
    cosmo : astropy.cosmology.core
    z0 : float
        Initial redshift.
    z1: float
        Final redshift.
    area: u.Quantity
        Survey area.

    """
    if z1 < z0:
        z0, z1 = z1, z0
    _ = np.pi * area.to(u.deg**2) / np.square(360 * u.deg)
    if isinstance(cosmo, ColossusCosmology) and cosmo.flat:
        cosmo = HybridFlatLambdaCDM(cosmo)
    assert isinstance(cosmo, AstropyCosmology), 'AstropyCosmology obj required.'
    return _ * (cosmo.comoving_volume(z1) - cosmo.comoving_volume(z0))


def wcs_pixel_info(sky):
    """
    Construct a World Coordinate System (WCS) object and return relevant pixel
    information for creating a fits image from the (ra,dec) positions of
    sources.

    For more information see the following links.

    Build a WCS structure programmatically
    http://docs.astropy.org/en/v0.2/wcs/index.html#building-a-wcs
    -structure-programmatically

    Writing out images with WCS information
    http://docs.hyperion-rt.org/en/stable/tutorials/tutorial_images.html

    Parameters
    ----------
    sky : SkyCoord

    Returns
    ----------
        w: The World Coordinate System (WCS) object.
        idx_sources: A tuple of pixel position values (sx, sy) converted from
        (ra, dec) of all the SExtracted sources within the
        boundaries of the dimensions of the frame (dim1, dim2).
        shape: The shape (a tuple of dimensions: (dim1, dim2)) that represents
        the shape of the axes for the frame.

    """
    # Get rough ima get dimensions
    ra, dec = sky.ra, sky.dec
    s1 = SkyCoord(np.nanmin(ra), np.median(dec))
    s2 = SkyCoord(np.nanmax(ra), np.median(dec))
    s3 = SkyCoord(np.median(ra), np.nanmin(dec))
    s4 = SkyCoord(np.median(ra), np.nanmax(dec))
    dim_ra = s1.separation(s2).to(u.arcmin)
    dim_dec = s3.separation(s4).to(u.arcmin)

    # Number of pixels.
    n_ra_pix = dim_ra.arcsec / 2
    n_dec_pix = dim_dec.arcsec / 2

    # Create a new WCS object. The number of axes must be set.
    w = wcs.WCS(naxis=2)
    # Use the center of the image as projection center.
    w.wcs.crpix = [n_ra_pix / 2, n_dec_pix / 2]
    # Set the pixel scale (in deg/pix).
    scale = 2 * u.arcsec.to(u.deg)
    w.wcs.cdelt = np.array([-scale, scale])
    # Set the coordinates of the image center.
    w.wcs.crval = [np.median(ra.deg), np.median(dec.deg)]
    # Set the coordinate system
    w.wcs.ctype = ["RA---TAN", "DEC--TAN"]

    # Convert the ra and dec np arrays of to arrays of the corresponding
    # pixel values in the newly defined wcs.

    # Note the third argument, set to 1, which indicates whether the pixel
    # coordinates should be treated as starting from (1, 1) (as FITS files
    # do) or from (0, 0).
    sx, sy = w.wcs_world2pix(ra, dec, 1)

    # Make sure there is a whole number of pixels in each pixel array.
    if np.nanmin(sx) < 0.5:
        w.wcs.crpix[0] = w.wcs.crpix[0] + 1 - np.nanmin(sx)
    if np.nanmin(sy) < 0.5:
        w.wcs.crpix[1] = w.wcs.crpix[1] + 1 - np.nanmin(sy)

    # dim1 and dim2 represent the dimensions of the axes for the frame.
    # sx, sy represent the pixel values of all the positions (converted from
    # ra, dec) of the SExtracted sources within the boundaries of dim1 and dim2.

    # Rerun world to pix conversion with new whole number values.
    # Note the third argument, set to 1, which indicates whether the pixel
    # coordinates should be treated as starting from (1, 1) (as FITS files
    # do) or from (0, 0).
    sx, sy = w.wcs_world2pix(ra, dec, 1)
    # Pixel arrays now contain whole numbers but need to be ints.
    sx, sy = np.rint(sx).astype(int), np.rint(sy).astype(int)
    idx_sources = sx, sy
    # Set dimensions of new data from max values in the pixel arrays.
    dim1, dim2 = 1 + np.amax(sx), 1 + np.amax(sy)
    shape = (dim1, dim2)
    return w, idx_sources, shape


def coord_meshgrid(data, wcs, skycoord=True):
    """
    Provides an RA and DEC for every single pixel of a 2D array given a world
    coordinate system. Primarily designed with the SkyCoord class in mind.


    Parameters
    ----------
    data - This should
    data
    wcs
    skycoord

    Returns
    -------

    """
    # based on https://github.com/astropy/astropy/issues/1587
    h, w = data.shape
    x, y, = np.arange(h), np.arange(w)
    x_mesh, y_mesh = np.meshgrid(x, y)
    ra, dec = wcs.wcs_pix2world(x_mesh, y_mesh, 0)
    if skycoord is True:
        return SkyCoord(ra, dec, unit='deg')
    else:
        return np.dstack((ra, dec))


def create_circular_mask(shape, center=None, radius=None):
    """
    based on https://stackoverflow.com/questions/44865023/circular-masking
    -an-image-in-python-using-numpy-arrays

    Parameters
    ----------
    shape
    center
    radius

    Returns
    -------

    """
    h, w = shape
    if center is None:
        # use the middle of the image
        center = [int(w / 2), int(h / 2)]
    if radius is None:
        # use the smallest distance between the center and image walls
        radius = min(center[0], center[1], w - center[0], h - center[1])

    y, x = np.ogrid[:h, :w]
    dist_from_center = np.sqrt((x - center[0]) ** 2 + (y - center[1]) ** 2)

    mask = dist_from_center <= radius
    return mask


def circle_skyregions_mask(regions, wcs, shape):
    """

    Parameters
    ----------
    regions - array-like, list of CircularSkyRegions
    regions
    wcs
    shape

    Returns
    -------

    """
    final_mask = np.zeros(shape=shape, dtype=bool)
    for sky_region in regions:
        pixel_region = sky_region.to_pixel(wcs=wcs)
        radius = pixel_region.radius
        center = pixel_region.center.x, pixel_region.center.y
        mask = create_circular_mask(shape=shape, center=center, radius=radius)
        final_mask = final_mask | mask
    return final_mask


def regions2compound(region_list):
    """
    Convert list of regions to a single compound region.

    Example
    -------
    Lists of regions are useful for area calculations but compound regions are
    way more useful for checking which sources lie within the regions.
    """

    for i, r in enumerate(region_list):
        if i == 0:
            compound_region = r
        else:
            compound_region = compound_region | r

    return compound_region
