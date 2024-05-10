#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A sub-module for reading / writing intermedian files.

Owner: Chunliang Mu
"""



# Init


#  import (my libs)
from ..log import say, is_verbose


#  import (general)
from datetime import datetime
from astropy import units
import os


CURRENT_VERSION = '0.5'


# Functions










# ---------------------------------- #
# -          helper funcs          - #
# ---------------------------------- #



def _add_metadata(
    metadata: None|dict = None, # dict|h5py.AttributeManager|None
    add_data: bool = True,
    verbose : int  = 3,
) -> dict: # dict|h5py.AttributeManager
    """Add additional info to meta data."""
    if metadata is None:
        metadata = {}
    if add_data:
        metadata['_version_clmuformatter_'] = CURRENT_VERSION
        now_time_utc = datetime.utcnow().isoformat()
        if '_created_time_utc_' not in metadata.keys():
            # only write if haven't already written
            metadata['_created_time_utc_' ] = now_time_utc
        else:
            metadata['_modified_time_utc_'] = now_time_utc
    return metadata



def get_str_from_astropyUnit(unit: units.Unit) -> str:
    """Translate astropy.units.core.Unit to string """
    # first test if the unit is a custom-defined unit that might not be parseable
    try:
        units.Unit(unit.to_string())
    except ValueError:
        unit = unit.cgs
    return unit.to_string()




def get_compress_mode_from_filename(filename: str, verbose: int = 3) -> str|bool:
    """Get the compress mode."""
    if not isinstance(filename, str):
        if is_verbose(verbose, 'fatal'):
            raise TypeError(f"Input filename should be of type str, but is of type {type(filename)=}.")
        return False
    _, ext = os.path.splitext(filename)

    if   ext in {  '.gz',  '.tgz'}:
        return 'gzip'
    elif ext in {'.hdf5', '.json'}:
        return False
    # fallback option
    elif is_verbose(verbose, 'warn'):
        say('warn', None, verbose,
            f"Unrecognized file extension {ext}. Proceeding without compression.")
        return False
