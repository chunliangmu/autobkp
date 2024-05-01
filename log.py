#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A sub-module for warning / error reporting / messaging things.

Owner: Chunliang Mu
"""

import inspect

# set default value of verboseness
DEFAULT_VERBOSE: int|bool = 3



def is_verbose(verbose: int|bool, verbose_req: None|int|str = 1) -> bool:
    """Test if we should be verbose.

    Accepted verbose_req input as str: (None or int are always okay)
        1: 'error' or 'err'
        2: 'warn'
        3: 'note'
        4: 'info' or 'debug_info'
    """
    if verbose_req is None or isinstance(verbose, bool):
        return verbose
    elif isinstance(verbose_req, str):
        if   verbose_req in {'fatal', 'err', 'error', 'Error'}:
            verbose_req = 1
        elif verbose_req in {'warn', 'warning', 'Warn', 'Warning'}:
            verbose_req = 2
        elif verbose_req in {'note', 'Note'}:
            verbose_req = 3
        elif verbose_req in {'info', 'debug', 'debug_info', 'Info', 'Debug'}:
            verbose_req = 4
        else:
            raise ValueError
    return verbose >= verbose_req





def say(
    level  : str,
    orig   : str|int|None,
    verbose: int|bool,
    *msgs  : str,
    sep    : str = '\n\t',
    verbose_req: int|None = None,
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
        

    verbose: int
        How much errors, warnings, notes, and debug info to be print on screen.
        * Note: Input type maybe exppand to accepting (int, file stream) as well in the future,
            to output to log files.

    msgs: str
        The messages to put up.

    sep : str
        Separator between the msgs.

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

    # get message
    if   level in {'fatal'}:
        msgs_txt = "*** Fatal  :"
    elif level in {'err', 'error', 'Error'}:
        msgs_txt = "*** Error  :"
    elif level in {'warn', 'warning', 'Warn', 'Warning'}:
        msgs_txt = "**  Warning:"
    elif level in {'note', 'Note'}:
        msgs_txt = "*   Note   :"
    elif level in {'info', 'debug', 'debug_info', 'Info', 'Debug'}:
        msgs_txt = "    Debug  :"
    else:
        raise ValueError
    msgs_txt += f"    {orig}:\n\t"
    msgs_txt += sep.join(msgs)

    if is_verbose(verbose, verbose_req):
        print(msgs_txt)
        
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
