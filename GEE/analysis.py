# -*- coding: utf-8 -*-
"""
Created on Tue Jul 22 13:15:54 2025

@author: emiles
"""

def flag_growth_trend(areas, dates, threshold_km2=0.05):
    if len(areas) < 2:
        return False
    growth = areas[-1] - areas[-2]
    return growth > threshold_km2
