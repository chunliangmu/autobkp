#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A sub-module for warning / error reporting / messaging things.

Owner: Chunliang Mu
"""

import inspect
#from typing import TextIO
import logging
from logging import Logger # type

# define the type of the verbose param
# if verbose is tuple: verbose[1] is the output direction
VerboseReqType = None|int|str
VerboseOutType = None|Logger    # output type - None -> print;  Logger -> logging.Logger
VerboseOutsType= set[VerboseOutType] | list[VerboseOutType] | tuple
VerboseOutsType_pure = set | list | tuple    # without generalization
VerboseType    = bool|int | tuple[bool|int, VerboseOutsType]
NumberType     = int |float

# set default value of verboseness
DEFAULT_VERBOSE: int|bool = 3

# translating the verbose_req from adhoc to normalized version
_VERBOSEREQDICT_TO_STR: dict[str, str] = {
    # Note: _VERBOSEREQDICT_TO_STR.values() must all be in keys()
    'None'      : 'None',
    
    'fatal'     : 'fatal',
    'CRITICAL'  : 'fatal',

    'err'       : 'err',
    'error'     : 'err',
    'Error'     : 'err',

    'warn'      : 'warn',
    'warning'   : 'warn',
    'Warn'      : 'warn',
    'Warning'   : 'warn',

    'note'      : 'note',
    'Note'      : 'note',

    'info'      : 'info',
    'Info'      : 'info',

    'debug'     : 'debug',
    'debug_info': 'debug',
    'Debug'     : 'debug',
}

# translating the verbose_req from normalized to int
_VERBOSEREQDICT_STR_TO_INT: dict[str, None|int] = {
    'None' : None,
    'fatal': 0,
    'err'  : 1,
    'warn' : 2,
    'note' : 3,
    'info' : 4,
    'debug': 5,
}

# translating the verbose_req from adhoc to int
_VERBOSEREQDICT_TO_INT: dict[str, None|int] = {
    req_user: _VERBOSEREQDICT_STR_TO_INT[req_norm]  for req_user, req_norm in _VERBOSEREQDICT_TO_STR.items()
}

# level for logging module
# note that logging uses a reversed level of verbose req
# i.e. the higher the more serious
_VERBOSEREQDICT_TO_LOGGING_LEVEL: dict[str, int] = {
    'None' : 99,
    'fatal': logging.CRITICAL,  # 50
    'err'  : logging.ERROR,     # 40
    'warn' : logging.WARNING,   # 30
    'note' : logging.INFO,      # 20
    'info' : logging.INFO,      # 20
    'debug': logging.DEBUG,     # 10
}
_VERBOSEREQDICT_TO_LOGGING_LEVEL = {
    req_user: _VERBOSEREQDICT_TO_LOGGING_LEVEL[req_norm]  for req_user, req_norm in _VERBOSEREQDICT_TO_STR.items()
}






def is_verbose(
    verbose: VerboseType,
    verbose_req: VerboseReqType = 1
) -> bool:
    """Test if we should be verbose.

    Accepted verbose_req input as str: (None or int are always okay)
        1: 'error' or 'err'
        2: 'warn'
        3: 'note'
        4: 'info' or 'debug_info'
    """
    if   isinstance(verbose_req, NumberType) and isinstance(verbose, NumberType):
        return verbose >= verbose_req
    elif verbose_req is None or isinstance(verbose, bool):
        return verbose
    elif isinstance(verbose, tuple|list):
        return is_verbose(verbose[0], verbose_req)
    elif isinstance(verbose_req, str):
        if verbose_req in _VERBOSEREQDICT_TO_INT.keys():
            return is_verbose(verbose, _VERBOSEREQDICT_TO_INT[verbose_req])
        else:
            raise ValueError(f"Unrecognized {verbose_req= }")

    raise TypeError(f"Unrecognized {verbose= } or {verbose_req= }")
        
    




def say(
    level  : str,
    orig   : None|int|str,
    verbose: VerboseType,
    *msgs  : str,
    sep    : str = '\n\t',
    end    : str = '\n',
    verbose_req: VerboseReqType = None,
) -> str:
    """Log a message as error, warning, note, or debug info.

    Will print the message if verbose > verbose_req (with verbose_req infered from level if None)
    
    Parameters
    ----------
    level: str
        Seriousness of the message. Acecptable input:
            'fatal', 'error' or 'err'
            'warn'
            'note'
            'info', 'debug', or 'debug_info'
        Will automatically set verbose_req if verbose_req is None.
    
    orig: str|int|None
        Origins of this message (typically function name).
        if None, will use orig=3.
        if int, will automatically use the names of the function (up to {orig} levels) that called this function.

        *** WARNING: orig as int|None does NOT work for @numba.jit decorated functions!!!
            In which case please mannually enter function name.
        

    verbose: int | tuple[int, set[None|TextIO]]
        How much errors, warnings, notes, and debug info to be print on screen.
        * Note: Input type maybe exppand to accepting (int, file stream) as well in the future,
            to output to log files.

    msgs: str
        The messages to put up.

    sep : str
        Separator between the msgs.

    end : str
        end character for printing the msgs.
        Does not get put in the returned str, nor is it used for logger.

    verbose_req: int
        Required minimum verbose to do anything.
        If 'None' (as str), will treat verbose as a bool and print even if verbose < 0 !
    """
    if verbose_req is None:
        verbose_req = level
    elif verbose_req in {'None'}:
        verbose_req = None

    # decide orig
    if orig is None:
        orig = 3
    if isinstance(orig, int):
        orig = '() ==> '.join([info.function for info in inspect.stack()][1:orig+1][::-1]) + '()'


    # normalize level
    level = _VERBOSEREQDICT_TO_STR[level]
    # get message
    if   level in {'fatal'}:
        msgs_txt = "*** Fatal  :"
    elif level in {'err'}:
        msgs_txt = "*** Error  :"
    elif level in {'warn'}:
        msgs_txt = "**  Warning:"
    elif level in {'note'}:
        msgs_txt = "*   Note   :"
    elif level in {'info', 'debug'}:
        msgs_txt = "    Debug  :"
    else:
        raise ValueError
    msgs_txt += f"    {orig}:\n\t"
    msgs_txt += sep.join(msgs)

    if is_verbose(verbose, verbose_req):
        # find output
        verbose_outs = {None,}
        if isinstance(verbose, tuple|list) and len(verbose) >= 2 and isinstance(verbose[1], VerboseOutsType_pure):
            verbose_outs = verbose[1]

        for verbose_out in verbose_outs:
            if verbose_out is None:
                print(msgs_txt, end=end)
            elif isinstance(verbose_out, Logger):
                verbose_out.log(_VERBOSEREQDICT_TO_LOGGING_LEVEL[level], msgs_txt)
            else:
                raise TypeError(f"Invalid {type(verbose_out)= }")
            
        
    return msgs_txt





def error(*args, verbose_req: int|None = 'None'):
    """***Deprecated*** Use say() instead. Show an Error message. """
    return say('err',   *args, verbose_req=verbose_req)


def warn(*args, verbose_req: int|None = 2):
    """***Deprecated*** Use say() instead. Show a warning message."""
    return say('warn',  *args, verbose_req=verbose_req)
    

def note(*args, verbose_req: int|None = 3):
    """***Deprecated*** Use say() instead. Show a note message."""
    return say('note',  *args, verbose_req=verbose_req)


def debug_info(*args, verbose_req: int|None = 4):
    """***Deprecated*** Use say() instead. Show a debug info message."""
    return say('debug', *args, verbose_req=verbose_req)
