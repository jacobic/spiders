from astropy import constants
def proper_velocity(zi, zclu):
    # Computes proper velocities of member galaxes in numpy array
    # zclu comes from clusterZ definition
    """
        zi: redshift of the members
        zclu: cluster redshift

        Returns array of velocities

        (Ruel et al. 2014)
        """
    return constants.c * (zi - zclu) / (1. + zclu)
