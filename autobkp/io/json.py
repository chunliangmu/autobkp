#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A sub-module for reading / writing intermedian json files.

Owner: Chunliang Mu
"""



# Init


#  import (my libs)
from ..log   import say, is_verbose
from ._shared import _add_metadata, get_str_from_astropyUnit, get_compress_mode_from_filename


#  import (general)
#import sys
import json
from json import JSONDecodeError    # do not delete this line, because other modules might need this
#import gzip
import numpy as np
from astropy import units
import io






# Functions


# custom type to json native types
STR_2_TYPE : dict[str, tuple] = {
    # name: (actual type, json type, other actual type aliases)
    "numpy.bool_": (np.bool_, bool),
    "numpy.int32": (np.int32, int, np.dtype('int32')),
    "numpy.int64": (np.int64, int, np.dtype('int64')),
    "numpy.float32": (np.float32, float, np.dtype('float32')),
    "numpy.float64": (np.float64, float, np.dtype('float64')),
}
TYPE_2_STR : dict[type|np.dtype, tuple[str, type]] = {ds[0]: (k, ds[1]) for k, ds in STR_2_TYPE.items()}
for k, ds in STR_2_TYPE.items():
    for d in ds[2:]:
        TYPE_2_STR[d] = (k, ds[1])
TYPE_2_STR_TYPES: tuple = tuple([t if isinstance(t, type) else type(t) for t in TYPE_2_STR.keys()])









# ---------------------------------- #
# - JSON-related read / write func - #
# ---------------------------------- #



#  suitable for small human-readable files

def _json_encode(
    obj     : dict,
    metadata: dict | None = {},
    overwrite_obj       : bool = False,
    overwrite_obj_kwds  : bool = False,
    ignore_unknown_types: bool = False,
    verbose : int = 3,
) -> dict:
    """Encode the obj to add meta data and do type convertion.

    Recursive. Note:

    1. DO NOT PUT NON-SERIALIZABLE THINGS IN LIST (NOT INPLEMENTED)! USE DICT INSTEAD.
    2. DO NOT INCLUDE THE FOLLOWING KEYWORDS IN INPUT: (they will be added by this func)
        '_meta_' : # meta data   (if top_level)
        '_data_' : # actual data (if top_level)
        '_type_' : # type of the data stored
            Supported type:
                 None|False (or other False-equivalent things): return '_data_' as is
               #'None'     : None.
                'np.bool_' : stored as bool (Will NOT read back as np.bool_ !)
               #'dict'     : dict
                'tuple': tuple stored as list
                'numpy.ndarray': numpy array stored as list by default
                'astropy.units.Quantity': astropy Quantity stored as list (value) and string (unit)
        '_unit_' : # unit of the astropy.units.Quantity, if that is the type
    
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
            if '_meta_' in obj.keys():
                # safety check
                if obj['_meta_'] and not overwrite_obj_kwds:
                    raise ValueError
                obj['_meta_'] = _add_metadata(obj['_meta_'], verbose=verbose)
            else:
                obj['_meta_'] = _add_metadata(verbose=verbose)
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
                say('warn', None, verbose,
                    "there are '_type_' keyword inside the input dict.",
                    "The data stored there will be removed to avoid issues.")
            else:
                say('warn', None, verbose,
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
        elif isinstance(obj, (type, np.dtype)) and obj in TYPE_2_STR_TYPES:
            obj = {
                '_type_': 'type',
                '_data_': TYPE_2_STR[obj][0],
            }
        elif isinstance(obj, TYPE_2_STR_TYPES):
            obj = {
                '_type_': TYPE_2_STR[type(obj)][0],
                '_data_': TYPE_2_STR[type(obj)][1](obj),
            }
        elif isinstance( obj, tuple ):
            obj = {'_type_': 'tuple', '_data_': list(obj)}
        elif type(obj) is np.ndarray:
            obj = {'_type_': 'numpy.ndarray', '_data_': obj.tolist()}
        elif type(obj) is units.Quantity:
            obj = {
                '_type_': 'astropy.units.Quantity',
                '_data_': obj.value.tolist(),
                '_unit_': get_str_from_astropyUnit(obj.unit),
            }
        else:
            if ignore_unknown_types:
                return "-NotImplemented-"
            else:
                raise NotImplementedError(f"_json_encode(): Unknown object type {type(obj)} from {obj = }")
    return obj





def _json_decode(
    obj     : dict,
    overwrite_obj : bool = False,
    load_metadata : bool = True,
    verbose : int  = 3,
) -> dict:
    """Decode the obj obtained from json_load(...) to its original state.

    Recursive.

    Parameters
    ----------
    obj: dict
        data to be serialized.

    overwrite_obj: bool
        If False, will copy the obj before modifying to avoid changing the raw data
            (This behavior is not guaranteed)
        
    load_metadata: bool
        Load meta data from loaded dict (top level only).
        
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
        if not load_metadata and isinstance(obj, dict) and '_meta_' in obj.keys():
            del obj['_meta_']
    
        # parse back to original data type
        if '_type_' in obj.keys():

            if not obj['_type_']:    # None
                if '_data_' in obj.keys():
                    return _json_decode(
                        obj['_data_'],
                        overwrite_obj=overwrite_obj,
                        load_metadata=True, verbose=verbose)
            elif obj['_type_'] == 'type' and '_data_' in obj.keys() and obj['_data_'] in STR_2_TYPE:
                return STR_2_TYPE[obj['_data_']][0]
            elif obj['_type_'] in STR_2_TYPE:
                if '_data_' in obj.keys():
                    return STR_2_TYPE[obj['_type_']][0](obj['_data_'])
            elif obj['_type_'] == 'tuple':
                if '_data_' in obj.keys():
                    return tuple(obj['_data_'])
            elif obj['_type_'] == 'numpy.ndarray':
                if '_data_' in obj.keys():
                    return np.array(obj['_data_'])
            elif obj['_type_'] == 'astropy.units.Quantity':
                if '_data_' in obj.keys() and '_unit_' in obj.keys():
                    return units.Quantity(value=obj['_data_'], unit=obj['_unit_'])#, copy=(not overwrite_obj))
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
                load_metadata=True, verbose=verbose)

    return obj





def json_dump(
    obj     : dict,
    fp      : io.BufferedReader,
    metadata: dict | None = {},
    overwrite_obj       : bool = False,
    overwrite_obj_kwds  : bool = False,
    ignore_unknown_types: bool = False,
    indent  : int | None = 1,
    verbose : int = 3,
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
    fp           : io.BufferedReader,
    load_metadata: bool = True,
    remove_metadata: bool|None = None,
    verbose      : int = 3,
):
    """Read obj from a json file (saved by json_dump(...) in this submodule).

    Parameters
    ----------
    fp: io.BufferedReader:
        File object you get with open(), with read permission.
        
    load_metadata: bool
        Load meta data from loaded dict.

    remove_metadata: bool|None
        *** Deprecated ***
        
    verbose: int
        How much erros, warnings, notes, and debug info to be print on screen.
    """
    if remove_metadata is not None:    # backward-compatibility term
        load_metadata = not remove_metadata
    return _json_decode( json.load(fp), overwrite_obj=True, load_metadata=load_metadata, verbose=verbose, )

