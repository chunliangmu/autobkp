#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A sub-module for reading / writing intermedian files.

Owner: Chunliang Mu
"""



# Init


#  import (my libs)
from .log import say, is_verbose


#  import (general)
import json
import numpy as np
from datetime import datetime
from astropy import units
import struct
import io


CURRENT_VERSION = '0.1'



# Functions


# ---------------------------------- #
# - JSON-related read / write func - #
# ---------------------------------- #

#  suitable for small human-readable files


def _json_encode(
    obj : dict,
    metadata            : dict|None = {},
    overwrite_obj       : bool      = False,
    overwrite_obj_kwds  : bool      = False,
    ignore_unknown_types: bool      = False,
    verbose            : int       = 1,
) -> dict:
    """Encode the obj to add meta data and do type convertion.

    Recursive. Note:

    1. DO NOT PUT NON-SERIALIZABLE THINGS IN LIST (NOT INPLEMENTED)! USE DICT INSTEAD.
    2. DO NOT INCLUDE THE FOLLOWING KEYWORDS IN INPUT: (they will be added by this func)
        '_meta_' : # meta data   (if top_level)
        '_data_' : # actual data (if top_level)
        '_type_' : # type of the data stored
            Supported type:
                 None  (or other False-equivalent things): return '_data_' as is
                'np.bool_' : stored as bool (Will NOT read back as np.bool_ !)
                'tuple': tuple stored as list
                'numpy.ndarray': numpy array stored as list by default
                'astropy.units.Quantity': astropy Quantity stored as list (value) and string (unit)
    
    Parameters
    ----------
    obj: dict
        data to be serialized.

    metadata: dict or None
        meta data to be added to file. The code will also save some of its own metadata.
        set it to None to disable this feature.
        
    overwrite_obj: bool
        If False, will copy the obj before modifying to avoid changing the raw data

    overwrite_obj_kwds: bool
        if to overwrite used keywords (see above) if it already exists.
        if False, may raise ValueError if used keywords already exists.

    ignore_unknown_types: bool
        If a data is not in the known list,
            replace the data with a message ("-NotImplemented-")
            instead of raising a NotImplementedError.
        
    verbose: int
        How much erros, warnings, notes, and debug info to be print on screen. 
        
    Returns
    -------
    obj: (as dict) serializable data
    """
    # first, make a copy
    if not overwrite_obj and isinstance(obj, dict):
        obj = obj.copy()

    # then, write metadata
    if metadata is not None:
        if isinstance(obj, dict):
            if '_meta_' in obj.keys() and obj['_meta_'] and not overwrite_obj_kwds:
                # safety check
                raise ValueError
            obj['_meta_'] = {
                '_version_myformatter_': CURRENT_VERSION,
                '_created_time_utc_': datetime.utcnow().isoformat(),
            }
            # note: no need to parse data since we will do it anyway in the next step
            if isinstance(metadata, dict):
                for key in metadata.keys():
                    obj['_meta_'][key] = metadata[key]
            else:
                obj['_meta_']['_data_'] = metadata
        else:
            return _json_encode(
                {'_type_': None, '_data_': obj}, metadata=metadata,
                overwrite_obj=overwrite_obj, overwrite_obj_kwds=overwrite_obj_kwds,
                ignore_unknown_types=ignore_unknown_types, verbose=verbose,)
    
    # now, parse regular data
    if isinstance(obj, dict):
        # safety check
        if '_type_' in obj.keys() and obj['_type_']:
            if overwrite_obj_kwds:
                del obj['_type_']
                say('warn', '_json_encode(...)', verbose,
                    "there are '_type_' keyword inside the input dict.",
                    "The data stored there will be removed to avoid issues.")
            else:
                say('warn', '_json_encode(...)', verbose,
                    "there are '_type_' keyword inside the input dict.",
                    "These could cause issues when reading data.")
        # recursively format whatever is inside the dict
        for key in obj.keys():
            obj[key] = _json_encode(
                obj[key], metadata=None,
                overwrite_obj=overwrite_obj, overwrite_obj_kwds=overwrite_obj_kwds,
                ignore_unknown_types=ignore_unknown_types, verbose=verbose,)
    else:
        # meaning this func is being recursively called- return the obj
        if isinstance( obj, (list, str, int, float, bool, type(None),) ):
            # native types
            pass
        # custom formatting
        #  *** Add new type here! ***
        elif isinstance( obj, np.bool_):
            obj = bool(obj)
        elif isinstance( obj, tuple ):
            obj = {'_type_': 'tuple', '_data_': list(obj)}
        elif type(obj) is np.ndarray :
            obj = {'_type_': 'numpy.ndarray', '_data_': obj.tolist()}
        elif type(obj) is units.Quantity :
            unit = obj.unit
            # first test if the unit is a custom-defined unit that might not be parseable
            try:
                units.Unit(unit.to_string())
            except ValueError:
                unit = unit.cgs
            obj = {
                '_type_': 'astropy.units.Quantity',
                '_data_': obj.value.tolist(),
                '_unit_': unit.to_string(),
            }
        else:
            if ignore_unknown_types:
                return "-NotImplemented-"
            else:
                raise NotImplementedError(f"_json_encode(): Unknown object type: {type(obj)}")
    return obj





def _json_decode(
    obj : dict,
    overwrite_obj   : bool = False,
    remove_metadata : bool = True,
    verbose        : int  = 1,
) -> dict:
    """Decode the obj obtained from json_load(...) to its original state.

    Recursive.

    Parameters
    ----------
    obj: dict
        data to be serialized.

    overwrite_obj: bool
        If False, will copy the obj before modifying to avoid changing the raw data
        
    remove_metadata: bool
        Remove meta data from loaded dict (top level only).
        
    verbose: int
        How much erros, warnings, notes, and debug info to be print on screen. 
        
    Returns
    -------
    obj: original data
    """


    if isinstance(obj, dict):
        
        # first, make a copy
        if not overwrite_obj and isinstance(obj, dict):
            obj = obj.copy()
    
        # then, remove metadata
        if remove_metadata and isinstance(obj, dict) and '_meta_' in obj.keys():
            del obj['_meta_']
    
        # parse back to original data type
        if '_type_' in obj.keys():

            if not obj['_type_']:    # None
                if '_data_' in obj.keys():
                    return _json_decode(
                        obj['_data_'],
                        overwrite_obj=overwrite_obj,
                        remove_metadata=False, verbose=verbose)
            elif obj['_type_'] == 'tuple':
                if '_data_' in obj.keys():
                    return tuple(obj['_data_'])
            elif obj['_type_'] == 'numpy.ndarray':
                if '_data_' in obj.keys():
                    return np.array(obj['_data_'])
            elif obj['_type_'] == 'astropy.units.Quantity':
                if '_data_' in obj.keys() and '_unit_' in obj.keys():
                    return units.Quantity(value=obj['_data_'], unit=obj['_unit_'], copy=(not overwrite_obj))
            else:
                say('warn', '_json_decode()', verbose,
                    f"Unrecognized obj['_type_']= {obj['_type_']}",
                    "type convertion for this is cancelled."
                     )
                    
            warn('_json_decode()', verbose,
                 "Found '_type_' keyword, but read failed." + \
                 "This could imply save file corruption." + \
                 " obj['_type_'] data ignored."
                 )
        for key in obj.keys():
            obj[key] = _json_decode(
                obj[key],
                overwrite_obj=overwrite_obj,
                remove_metadata=False, verbose=verbose)

    return obj





def json_dump(
    obj: dict,
    fp: io.BufferedReader,
    metadata: dict|None = {},
    overwrite_obj = False,
    overwrite_obj_kwds = False,
    ignore_unknown_types: bool = False,
    indent: int|None = 1,
    verbose: int = 1,
):
    """Dump obj to file-like fp as a json file in my custom format with support of numpy arrays etc.

    Suitable for storing small human-readable files.


    Parameters
    ----------
    obj: dict
        data to be serialized.

    fp: io.BufferedReader:
        File object you get with open(), with write permission.
        
    metadata: dict | None
        meta data to be added to file. The code will also save some of its own metadata.
        set it to None to disable this feature.
        
    overwrite_obj: bool
        If False, will copy the obj before modifying to avoid changing the raw data

    overwrite_obj_kwds: bool
        if to overwrite used keywords (see above) if it already exists.
        if False, may raise ValueError if used keywords already exists.
        
    ignore_unknown_types: bool
        If a data is not in the known list,
            replace the data with a message ("-NotImplemented-")
            instead of raising a NotImplementedError.
        
    indent: int | None
        indentation in the saved json files.
        
    verbose: int
        How much erros, warnings, notes, and debug info to be print on screen.
    """
    obj = _json_encode(
        obj, metadata=metadata,
        overwrite_obj=overwrite_obj, overwrite_obj_kwds=overwrite_obj_kwds,
        ignore_unknown_types=ignore_unknown_types, verbose=verbose,)
    return json.dump( obj, fp, indent=indent, )



def json_load(
    fp: io.BufferedReader,
    remove_metadata: bool = True,
    verbose: int = 1,
):
    """Read obj from a json file (saved by json_dump(...) in this submodule).

    Parameters
    ----------
    fp: io.BufferedReader:
        File object you get with open(), with read permission.
        
    remove_metadata: bool
        remove meta data from loaded dict.
        
    verbose: int
        How much erros, warnings, notes, and debug info to be print on screen.
    """
    return _json_decode( json.load(fp), overwrite_obj=True, remove_metadata=True, verbose=verbose, )



# ----------------------------- #
# - Fortran-related read func - #
# ----------------------------- #

def fortran_read_file_unformatted(
    fp: io.BufferedReader,
    t: str,
    no: int|None = None,
    verbose: int = 3,
) -> tuple:
    """Read one record from an unformatted file saved by fortran.

    Because stupid fortran save two additional 4 byte int before and after each record respectively when saving unformatted data.
    (Fortran fans please don't hit me)

    Parameters
    ----------
    fp: io.BufferedReader:
        File object you get with open(), with read permission.

    t: str
        Type of the data. Acceptable input:
            'i' | 'int'    | 'integer(4)': 4-byte integer
            'f' | 'float'  | 'real(4)'   : 4-byte float
            'd' | 'double' | 'real(8)'   : 8-byte float
    no: int|None
        Number of data in this record. if None, will infer from record.
        
    verbose: int
        How much erros, warnings, notes, and debug info to be print on screen.
    """

    if t in ['i', 'int', 'integer(4)']:
        t_format = 'i'
        t_no_bytes = 4
    elif t in ['f', 'float', 'real(4)']:
        t_format = 'f'
        t_no_bytes = 4
    elif t in ['d', 'double', 'real(8)']:
        t_format = 'd'
        t_no_bytes = 8
    else:
        say('err',
            'fortran_read_file_unformatted()', verbose,
            f"Unrecognized data type t={t}."
            )
        raise NotImplementedError
    
    rec_no_bytes = struct.unpack('i', fp.read(4))[0]
    no_in_record = int(rec_no_bytes / t_no_bytes)
    if no is None:
        no = no_in_record
        rec_no_bytes_used = rec_no_bytes
    else:
        rec_no_bytes_used = no * t_no_bytes
        if no != no_in_record:
            say('warn', 'fortran_read_file_unformatted()', verbose,
                f"Supplied no={no} does not match the record no_in_record={no_in_record}.",
                "Incorrect type perhaps?",
                "will continue to use supplied no regardless."
            )

    data = struct.unpack(f'{no}{t_format}', fp.read(rec_no_bytes_used))
    rec_no_bytes_again = struct.unpack('i', fp.read(4))[0]
    if rec_no_bytes != rec_no_bytes_again:
        say('warn', 'fortran_read_file_unformatted()', verbose,
            "The no of bytes recorded in the beginning and the end of the record did not match!",
            f"Beginning is {rec_no_bytes}, while end is {rec_no_bytes_again}.",
            "This means something is seriously wrong.",
            "Please Check if data sturcture is correct and file is not corrupted.",
        )
    return data

