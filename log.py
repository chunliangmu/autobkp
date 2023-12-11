#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A sub-module for warning / error reporting / messaging things.

Owner: Chunliang Mu
"""



def is_verbose(verbose: int|bool, verbose_req: None|int|str) -> bool:
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
        if verbose_req in {'err', 'error', 'Error'}:
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
    orig   : str,
    verbose: int|bool,
    *msgs  : str,
    verbose_req: int|None = None,
) -> str:
    """Log a message as error, warning, note, or debug info.

    Will print the message if verbose > verbose_req (with verbose_req infered from level if None)
    
    Parameters
    ----------
    level: str
        Seriousness of the message. Acecptable input:
            'error' or 'err'
            'warn'
            'note'
            'info', 'debug', or 'debug_info'
        Will automatically set verbose_req if verbose_req is None.
    
    orig: str
        Origins of this message (typically function name).

    verbose: int
        How much errors, warnings, notes, and debug info to be print on screen.
        * Note: Input type maybe exppand to accepting (int, file stream) as well in the future,
            to output to log files.

    msgs: str
        The messages to put up.

    verbose_req: int
        Required minimum verbose to do anything.
        If 'None' (as str), will treat verbose as a bool and print even if verbose < 0 !
    """
    if verbose_req is None:
        verbose_req = level
    elif verbose_req in {'None'}:
        verbose_req = None

    # get message
    if   level in {'err', 'error', 'Error'}:
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
    msgs_txt += '\n\t'.join(msgs)

    if is_verbose(verbose, verbose_req):
        print(msgs_txt)
        
    return msgs_txt





def error(*args, verbose_req: int|None = 'None'):
    """Show an Error message. Deprecated. Use say() instead."""
    return say('err',   *args, verbose_req=verbose_req)


def warn(*args, verbose_req: int|None = 2):
    """Show a warning message. Deprecated. Use say() instead."""
    return say('warn',  *args, verbose_req=verbose_req)
    

def note(*args, verbose_req: int|None = 3):
    """Show a note message. Deprecated. Use say() instead."""
    return say('note',  *args, verbose_req=verbose_req)


def debug_info(*args, verbose_req: int|None = 4):
    """Show a debug info message. Deprecated. Use say() instead."""
    return say('debug', *args, verbose_req=verbose_req)
