import numpy as np


def lat_lon_distance(lat1, lon1, lat2, lon2):
    """
    Calculates the distance in km between two lat-lon points
    """
    try:
        p = 0.017453292
        a = 0.5 - np.cos((lat2 - lat1) * p)/2 + np.cos(lat1 * p) * np.cos(lat2 * p) * (1 - np.cos((lon2 - lon1) * p)) / 2
        return round(12742 * np.arcsin(np.sqrt(a)),1)
    except:
        print(f'error with:, {lat1}, {lon1} - {lat2}, {lon2}')


def sample_points_in_square(upper_left: tuple,
                            lower_right: tuple,
                            n: int=5)->list:
    """
    Get n^2 points evenly spaced within a defined square
    Useful to sample from a known lat/long area
    """
    
    longs = list(np.linspace(upper_left[1], lower_right[1], num=n))
    lats = list(np.linspace(upper_left[0], lower_right[0], num=n))
    coords = [(y,x) for y in lats for x in longs]
    
    return coords

