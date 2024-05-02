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
import sys
import json
from json import JSONDecodeError
import h5py
import gzip
import numpy as np
from datetime import datetime
from astropy import units
import struct
import io
import os
import shutil


CURRENT_VERSION = '0.5'

HDF5_ATTRS_ACCEPTABLE_TYPES : tuple = (
    int, float, str,
    bool, np.bool_,
    np.float32, np.float64,
)




# Functions










# ---------------------------------- #
# -          helper funcs          - #
# ---------------------------------- #



def _add_metadata(
    metadata: dict|h5py.AttributeManager|None = None,
    add_data: bool = True,
    verbose : int  = 3,
) -> dict|h5py.AttributeManager:
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
        elif isinstance( obj, np.bool_):
            obj = bool(obj)
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
                raise NotImplementedError(f"_json_encode(): Unknown object type: {type(obj)}")
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










# ---------------------------------- #
# - HDF5-related read / write func - #
# ---------------------------------- #




def _hdf5_dump_metadata(
    metadata: dict,
    grp     : h5py.Group,
    add_data: bool = True,
    verbose : int = 3,
) -> None:
    """Dump metadata to grp.attrs.

    Parameters
    ----------
    metadata: dict
        data to be written.
        If not dict, will raise Error if verbose, or do NOTHING if otherwise.

    grp: h5py.File | h5py.Group
        hdf5 data file, where data will be written to.

    add_data: bool
        Add additional metadata info.
        
    verbose: int
        How much erros, warnings, notes, and debug info to be print on screen.
    """
    if isinstance(metadata, dict):
        # safety check
        if is_verbose(verbose, 'warn') and sys.getsizeof(metadata) + sys.getsizeof(grp.attrs) >= 65535:
            say('warn', None, verbose,
                "Potentially large metadata size:",
                f"(Adding {sys.getsizeof(metadata)/1024:.1f}KB to {sys.getsizeof(grp.attrs)/1024:.1f}KB).",
                "Should be less than 64KB.",
                sep=' ',
            )
        _add_metadata(grp.attrs, add_data=add_data, verbose=verbose)
        for key in metadata.keys():
            if   isinstance(metadata[key], dict):
                # add metadata to individual datasets
                if key in grp.keys():
                    _hdf5_dump_metadata(metadata[key], grp[key], add_data=False, verbose=verbose)
                elif is_verbose(verbose, 'err'):
                    say('err', None, verbose, f"{key=} in {metadata.keys()=}, but not in {grp.keys()}.")
            elif isinstance(metadata[key], HDF5_ATTRS_ACCEPTABLE_TYPES):
                grp.attrs[key] = metadata[key]
            elif is_verbose(verbose, 'err'):
                say('err', None, verbose, f"Unexpected metadata[{key=}] type: {type(metadata[key])}.")
    else:
        if is_verbose(verbose, 'fatal'):
            raise TypeError(f"metadata {type(metadata)=} should be of type 'dict'.")
    return



def _hdf5_dump_sub(
    data    : dict,
    grp     : h5py.Group,
    metadata: dict|None = {},
    add_metadata: bool = True,
    verbose : int = 3,
) -> None:
    """Encode the data and dump to grp.

    Suitable for storing medium/large machine-readable files.

    Do NOT put weird characters like '/' in obj.keys().
    obj['_meta_'] will be stored as metadata in grp.attrs.

    Parameters
    ----------
    data: dict
        data to be written.

    grp: h5py.File | h5py.Group
        hdf5 data file, where data will be written to.
        
    metadata: dict | None
        meta data to be added to file. The code will also save some of its own metadata.
        set it to None to disable this feature.

    add_metadata: bool
        Add additional metadata info.
        
    verbose: int
        How much erros, warnings, notes, and debug info to be print on screen.
    """
    
    # write data to file
    if isinstance(data, dict):
        
        
        for key in data.keys():
            obj = data[key]
            
            # sanity check
            if is_verbose(verbose, 'fatal') and not isinstance(key, str):
                # must be in str because it's the folder path within hdf5 files
                raise TypeError(f"key={key} of dict 'data' should be of type 'str', but it is of type {type(key)}.")

            # hold for metadata
            if key in {'_meta_'}:
                # wait till after to write in case we want to write metadata to some of the datasets too
                pass
            else:
                # parse into data and dump
                
                if   isinstance(obj, type(None)):
                    sav = grp.create_dataset(key, dtype='f')
                    sav.attrs['_type_'] = 'None'
                    
                elif isinstance(obj, HDF5_ATTRS_ACCEPTABLE_TYPES):
                    if ('_meta_' in data.keys() and key in data['_meta_'].keys()) or (isinstance(metadata, dict) and key in metadata.keys()):
                        sav = grp.create_dataset(key, dtype='f')
                        sav.attrs['_data_'] = obj
                        sav.attrs['_type_'] = False
                    else:
                        sav = grp['_misc_'] if '_misc_' in grp.keys() else grp.create_dataset('_misc_', dtype='f')
                        sav.attrs[  key   ] = obj

                elif isinstance( obj, (tuple, list) ):
                    obj_elem_type = type(obj[0])
                    np_array_like = issubclass(obj_elem_type, (float, int))
                    # check type coherence
                    for obj_elem in obj:
                        if obj_elem_type != type(obj_elem):
                            np_array_like = False
                    if np_array_like:
                        sav = grp.create_dataset(key, data=np.array(obj))
                        sav.attrs['_type_'] = 'numpy.ndarray'
                    else:
                        sav = grp.create_group(key)
                        _hdf5_dump_sub({str(i): iobj for i, iobj in enumerate(obj)}, sav, metadata=None, add_metadata=False, verbose=verbose)
                        sav.attrs['_type_'] = 'tuple'
                        
                elif type(obj) is np.ndarray:
                    if len(obj.shape):    # array-like
                        sav = grp.create_dataset(key, data=obj)
                    else:                 # scalar
                        sav = grp.create_dataset(key, dtype='f')
                        sav.attrs['_data_'] = obj.item()
                    sav.attrs['_type_'] = 'numpy.ndarray'
                        
                elif type(obj) is units.Quantity:
                    if len(obj.shape):    # array-like
                        sav = grp.create_dataset(key, data=obj.value)
                    else:                 # scalar
                        sav = grp.create_dataset(key, dtype='f')
                        sav.attrs['_data_'] = obj.value
                    sav.attrs['_type_'] = 'astropy.units.Quantity'
                    sav.attrs['_unit_'] = get_str_from_astropyUnit(obj.unit)

                elif isinstance(obj, dict):
                    sav = grp.create_group(key)
                    _hdf5_dump_sub(obj, sav, metadata=None, add_metadata=False, verbose=verbose)
                    sav.attrs['_type_'] = 'dict'
                    
                else:
                    # Not yet implemented
                    if is_verbose(verbose, 'fatal'):
                        raise NotImplementedError(f"I haven't yet implemented storing data type {type(obj)} in hdf5.")

                
        if '_meta_' in data.keys():
            # write meta data as of the data
            _hdf5_dump_metadata(data['_meta_'], grp, add_data=add_metadata, verbose=verbose)
            
    else:
        if is_verbose(verbose, 'fatal'):
            raise TypeError(f"Incorrect input type of data: {type(data)}. Should be dict.")

    # write more metadata
    if metadata is not None:
        _hdf5_dump_metadata(metadata, grp, add_data=add_metadata, verbose=verbose)

    return





def _hdf5_load_sub(
    data    : dict,
    grp     : h5py.Group,
    load_metadata : bool = True,
    verbose : int = 3,
) -> dict:
    """load from grp, decode and put into data.

    Suitable for storing medium/large machine-readable files.

    Do NOT put weird characters like '/' in obj.keys().
    obj['_meta_'] will be stored as metadata in grp.attrs.

    Parameters
    ----------
    data: dict
        dict to be load into.

    grp: h5py.File | h5py.Group
        hdf5 data file, where data will be load from.
        
    load_metadata : bool
        Do NOT load meta data from loaded dict.
        
    verbose: int
        How much erros, warnings, notes, and debug info to be print on screen.

        
    Returns
    -------
    data: original data
    """
    
    # re-construct data from file
    if isinstance(data, dict):
        
        if load_metadata:
            data['_meta_'] = dict(grp.attrs)

        for key in grp.keys():
            
            obj = grp[key]

            if   isinstance(obj, h5py.Group  ):    # is dict

                if load_metadata:
                    data['_meta_'][key] = dict(obj.attrs)
                
                data[key] = {}
                _hdf5_load_sub(data[key], obj, load_metadata=load_metadata, verbose=verbose)


                if '_type_' in obj.attrs.keys() and obj.attrs['_type_'] in {'tuple'}:
                    try:
                        data_temp = {k: v for k, v in data[key].items() if k not in {'_meta_'}}
                        data[key] = tuple([data_temp[i] for i in sorted(data_temp, key=lambda x: int(x))])
                    except ValueError:
                        if is_verbose(verbose, 'err'):
                            say('err', None, verbose,
                                f"Unexpected input: cannot convert {key=} from dict to tuple,",
                                f"because {data[key].keys()=} cannot each be converted to integers.")
                
            elif isinstance(obj, h5py.Dataset):    # is data


                if load_metadata and key not in {'_misc_'}:   # load metadata
                    data['_meta_'][key] = dict(obj.attrs)

                
                if key in {'_misc_'}: # is small pieces of data
                    for k in obj.attrs.keys():
                        data[k] = obj.attrs[k]
                        
                elif obj.shape:       # is array

                    data[key] = obj
                    if '_type_' in obj.attrs.keys():
                        if   obj.attrs['_type_'] in {'numpy.ndarray'}:
                            data[key] = np.array(data[key])
                        elif obj.attrs['_type_'] in {'astropy.units.Quantity'} and '_unit_' in obj.attrs.keys():
                            data[key] = units.Quantity(value=data[key], unit=obj.attrs['_unit_'])
                        elif is_verbose(verbose, 'err'):
                            say('err', None, verbose, f"Unexpected input {dict(obj.attrs)=}")
                    elif is_verbose(verbose, 'err'):
                        say('err', None, verbose, f"Unexpected input {dict(obj.attrs)=}")

                else:                 # is scalar
                    
                    # load data
                    data[key] = None
                    if   '_data_' in obj.attrs.keys():
                        data[key] = obj.attrs['_data_']
                    elif is_verbose(verbose, 'err') and '_type_' not in obj.attrs.keys():
                        say('err', None, verbose, f"Unexpected input {dict(obj.attrs)=}")

                    # re-construct data
                    if '_type_' in obj.attrs.keys() and obj.attrs['_type_']:
                        if   obj.attrs['_type_'] in {'None'}:
                            data[key] = None
                        elif obj.attrs['_type_'] in {'numpy.ndarray'}:
                            data[key] = np.array(data[key])
                        elif obj.attrs['_type_'] in {'astropy.units.Quantity'} and '_unit_' in obj.attrs.keys():
                            data[key] = units.Quantity(value=data[key], unit=obj.attrs['_unit_'])
                        elif is_verbose(verbose, 'err'):
                            say('err', None, verbose, f"Unexpected input {dict(obj.attrs)=}")
                
            elif is_verbose(verbose, 'err'):
                say('err', None, verbose, f"Unexpected input type {type(obj)=}")


    return data



def hdf5_open(
    filename: str,
    filemode: str = 'a',
    metadata: None|dict = None,
    compress: None|bool = None,
    verbose : int = 3,
) -> h5py.File:
    """Open a hdf5 file.

    Remember to close it with the .close() function!
    Alternatively you can put this in a with group.

    You can write to sub groups within one file by running
        hdf5_dump(obj, fp.create_group([group_name]))


    Parameters
    ----------
    compress: None | bool | 'gzip'
        if the file is compressed.
        if None, will guess from file name.
        if is True, will use 'gzip'.
        Will do nothing if fp is not of type str.
    """
    # compression
    if compress is None and isinstance(filename, str):
        compress = get_compress_mode_from_filename(filename)
    if compress:
        if   filemode in {'r'}:
            filename = gzip.open(filename, f'{filemode[0]}b')
        elif filemode in {'a'}:
            # decompress whole file before writing
            filename = gzip.open(filename, f'{filemode[0]}b')
            filename_root, ext = os.path.splitext(filename)
            if ext not in {'.gz'} and is_verbose(verbose, 'fatal'):
                raise ValueError(f"{filename=} should end with extension '.gz'.")
            with gzip.open(filename, 'rb') as f_in, open(filename_root, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            filename = filename_root
            if is_verbose(verbose, 'note'):
                say('note', None, f"Remember to manually compress the file, or use hdf5_close()")
        elif filemode in {'w', 'x'}:
            filename_root, ext = os.path.splitext(filename)
            if ext in {'.gz'}:
                filename = filename_root
            if is_verbose(verbose, 'note'):
                say('note', None, f"Remember to manually compress the file, or use hdf5_close()")
        elif is_verbose(verbose, 'fatal'):
            raise ValueError(f"Unrecognized {filemode=}")
    
    
    fp = h5py.File(filename, mode=filemode)
    if metadata is not None:
        _hdf5_dump_sub({}, fp, metadata, add_metadata=True, verbose=verbose)
    return fp





def hdf5_close(
    fp      : h5py.File,
    compress: None|bool = None,
    verbose : int = 3,
) -> None:
    fp.close()
    if compress is None or compress:
        raise NotImplementedError("Compression in this func not yet implemented")





def hdf5_subgroup(
    fp       : h5py.File | h5py.Group,
    grp_name : str,
    metadata : None|dict = None,
    overwrite: bool= False,
    verbose  : int = 3,
) -> h5py.Group:
    """Create / get a subgroup from fp.
    
    Remember to set overwrite=True at the dump-level.
    """
    
    if overwrite and grp_name in fp.keys():
        del fp[grp_name]
        
    fp_subgrp = fp[grp_name] if grp_name in fp.keys() else fp.create_group(grp_name)
    
    if metadata is not None:
        _hdf5_dump_sub({}, fp_subgrp, metadata, add_metadata=True, verbose=verbose)
        
    return fp_subgrp



def hdf5_dump(
    obj     : dict,
    fp      : str | h5py.File | h5py.Group,
    metadata: None| dict = None,
    compress: None| bool | str = None,
    verbose : int = 3,
) -> None:
    """Dump obj to file-like fp as a hdf5 file in my custom format with support of numpy arrays etc.

    *** WILL OVERWRITE EXISTING FILES ***

    Suitable for storing medium/large machine-readable files.

    Do NOT put weird characters like '/' in obj.keys().

    DO NOT INCLUDE THE FOLLOWING KEYWORDS IN INPUT UNLESS YOU KNOW WHAT YOU ARE DOING
        '_meta_' : # meta data
            Note: You can add additional metadata for each datasets, in format of e.g.
            data = {
                'x1': ...,
                'x2': ...,
                ...,
                '_meta_': {
                    'x1': { 'Description': "Description of x1.", },
                    'x2': { 'Description': "Description of x2.", },
                },
            }
        '_data_' : # actual data
        '_type_' : # type of the data stored
            Supported type:
                 None|False  (or other False-equivalent things): return '_data_' as is
                'None'     : None.
                'str'      : Sting
                'dict'     : dict
                'np.bool_' : stored as bool (Will NOT read back as np.bool_ !)
                'tuple': tuple stored as list
                'numpy.ndarray': numpy array stored as list by default
                'astropy.units.Quantity': astropy Quantity stored as list (value) and string (unit)
        '_unit_' : unit of the astropy.units.Quantity, if that is the type
        '_misc_' : small pieces of data
        

    Parameters
    ----------
    obj: dict
        data to be written.

    fp: io.BufferedReader:
        File object you get with open(), with write permission.
        
    metadata: dict | None
        meta data to be added to file. The code will also save some of its own metadata.
        set it to None to disable this feature.

    compress: None | bool | 'gzip'
        if the file is compressed.
        if None, will guess from file name.
        if is True, will use 'gzip'.
        Will do nothing if fp is not of type str.
        
    verbose: int
        How much erros, warnings, notes, and debug info to be print on screen.
    """
    if metadata is None:
        metadata = {}
    if isinstance(fp, str):
        
        # init
        if compress is None:
            compress = get_compress_mode_from_filename(fp)
        filename_root, ext = os.path.splitext(fp)
        if not compress or ext not in {'.gz'}:
            filename_root = fp
        if is_verbose(verbose, 'note'):
            say('note', None, verbose,
                f"Writing to {filename_root}  (will OVERWRITE if file already exist.; {compress=})")
        # open & dump
        with h5py.File(filename_root, mode='w') as f:
            _hdf5_dump_sub(obj, f, metadata, add_metadata=True, verbose=verbose)
            
        # compress
        if compress:
            #if compress in {'gzip'}:
            if is_verbose(verbose, 'note'):
                say('note', None, verbose,
                    f"Compressing and saving to {filename_root}.gz;",
                    f"Deleting {filename_root}",
                )
            with open(filename_root, 'rb') as f_in, gzip.open(f"{filename_root}.gz", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(filename_root)
            #else:
            #    if is_verbose(verbose, 'warn'):
            #        say('warn', None, f"Unrecognized compress mode {compress=}, will read as if compress=False")

                
    elif isinstance(fp, h5py.Group):
        _hdf5_dump_sub(obj, fp, metadata, add_metadata=True, verbose=verbose)
    elif is_verbose(verbose, 'fatal'):
        raise TypeError(f"Unexpected input fp type {type(fp)=}")
    return





def hdf5_load(
    fp      : str | h5py.File | h5py.Group,
    load_metadata : bool = False,
    compress: None| bool | str = None,
    verbose : int = 3,
) -> None:
    """Load data from h5py file in my custom format.


    Parameters
    ----------
    fp: io.BufferedReader:
        File object you get with open(), with write permission.
        
    load_metadata : bool
        Do NOT load meta data from loaded dict.

    compress: None | bool | 'gzip'
        if the file is compressed.
        if None, will guess from file name.
        if is True, will use 'gzip'.
        Will do nothing if fp is not of type str.
        
    verbose: int
        How much erros, warnings, notes, and debug info to be print on screen.

    Returns
    -------
    obj: original data
    """
    if isinstance(fp, str):
        
        # init
        if compress is None:
            compress = get_compress_mode_from_filename(fp)
        if is_verbose(verbose, 'note'):
            say('note', None, verbose, f"Reading from {fp}  ({compress=})")

        # open & read
        do_compress = False
        if compress:
            #if compress in {'gzip'}:
            do_compress = True
            with h5py.File(gzip.open(fp, 'rb'), mode='r') as f:
                obj = _hdf5_load_sub({}, f, load_metadata=load_metadata, verbose=verbose)
            #else:
            #    if is_verbose(verbose, 'warn'):
            #        say('warn', None, f"Unrecognized compress mode {compress=}, will read as if compress=False")
        if not do_compress:
            # no compression
            with h5py.File(fp, mode='r') as f:
                obj = _hdf5_load_sub({}, f, load_metadata=load_metadata, verbose=verbose)
                
    elif isinstance(fp, h5py.Group):
        obj = _hdf5_load_sub({}, fp, load_metadata=load_metadata, verbose=verbose)
    elif is_verbose(verbose, 'fatal'):
        raise TypeError(f"Unexpected input fp type {type(fp)=}")

    return obj









# ----------------------------- #
# - Fortran-related read func - #
# ----------------------------- #



def fortran_read_file_unformatted(
    fp: io.BufferedReader,
    t: str,
    no: None|int = None,
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
        say('err', None, verbose,
            f"Unrecognized data type t={t}."
        )
        if is_verbose(verbose, 'fatal'):
            raise NotImplementedError
    
    rec_no_bytes = struct.unpack('i', fp.read(4))[0]
    no_in_record = int(rec_no_bytes / t_no_bytes)
    if no is None:
        no = no_in_record
        rec_no_bytes_used = rec_no_bytes
    else:
        rec_no_bytes_used = no * t_no_bytes
        if no != no_in_record:
            say('warn', None, verbose,
                f"Supplied no={no} does not match the record no_in_record={no_in_record}.",
                "Incorrect type perhaps?",
                "will continue to use supplied no regardless."
            )

    data = struct.unpack(f'{no}{t_format}', fp.read(rec_no_bytes_used))
    rec_no_bytes_again = struct.unpack('i', fp.read(4))[0]
    if rec_no_bytes != rec_no_bytes_again:
        say('warn', None, verbose,
            "The no of bytes recorded in the beginning and the end of the record did not match!",
            f"Beginning is {rec_no_bytes}, while end is {rec_no_bytes_again}.",
            "This means something is seriously wrong.",
            "Please Check if data sturcture is correct and file is not corrupted.",
        )
    return data

