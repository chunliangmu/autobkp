#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""A script to auto-backup data.

Author: Chunliang Mu
"""


from .log import say, is_verbose
from .readwrite import json_dump, json_load, JSONDecodeError

import logging
import os
from os.path import sep
import shutil
import filecmp
from datetime import datetime
import time
import gzip
import numpy as np





def _get_timestamp_str(timestamp: float) -> str:
    """Get the str version of time. Returns value in utc and is semi-human-readable.
    """
    return datetime.utcfromtimestamp(timestamp).strftime("%Y%m%d%H%M%S%f")


def _get_timestamp_px6(timestamp: float) -> int:
    """Get the int version of time. Returns value in utc and is semi-human-readable.
    """
    # x1000000 to include the microseconds
    return int(datetime.utcfromtimestamp(timestamp).timestamp()*1e6)


def _get_timestamp_str_from_px6(timestamp_px6: int) -> str:
    """Get the str version of time. Returns value in utc and is semi-human-readable.
    """
    return datetime.fromtimestamp(timestamp_px6/1e6).strftime("%Y%m%d%H%M%S%f")





def _get_bkp_filename(dst_path: str, mtime_utc: str, compress: bool|str = False, verbose: int=4) -> str:
    """f-string combine dst path and mtime into backup file name.
    """
    dst_path = os.path.normpath(dst_path)
    dst_path_new = f'{dst_path}.bkp{mtime_utc}._bkp_'
    if not compress or compress in {'gztar'}:
        # do not add file extension
        pass
    elif compress in {'gzip'}:
        dst_path_new += '.gz'
    elif is_verbose(verbose, 'err'):
        say('err', None, verbose, f"Unknown compression method '{compress}'. Will assume no extra file extension")
    return dst_path_new





def _get_dir_metadata(src_path: str) -> dict:
    """Recursively get the metadata (newest mtime & total size) for a dir.

    Ignores things in symbolic links.

    dst_path: str
        path to a file. Must not end with '/'. (Does not check that)
    
    """
    data = {
        'size' : os.path.getsize( src_path),    # int
        'mtime': os.path.getmtime(src_path),    # float
    }
    if os.path.isdir(src_path):
        for filename in os.listdir(src_path):
            new_data = _get_dir_metadata(f'{src_path}{sep}{filename}')
            data['size'] += new_data['size']
            if new_data['mtime'] > data['mtime']:
                data['mtime'] = new_data['mtime']
    return data





def _save_bkp_file(
    src_path: str,
    dst_path: str,
    action  : str  = 'copy',
    dry_run : bool = False,
    compress: bool|str = False,
    verbose : int  = 4,
):
    """Save source file to the destination file.    
    """

    if not compress:
        if action in {'copy', 'Copy', 'cp', 'move', 'Move', 'mv'}:
            if is_verbose(verbose, 'note'):
                say('note', None, verbose, f"Copying '{src_path}' to '{dst_path}'")
            if not dry_run:
                shutil.copy2(src_path, dst_path, follow_symlinks=False)
            #if action in {'move', 'Move', 'mv'}:
            #    if is_verbose(verbose, 'note'):
            #        say('note', None, verbose, f"Removing '{src_path}'")
            #    if not dry_run:
            #        os.remove(src_path)
            return
    
    elif compress in {'gzip'}:
        # sanity check
        if is_verbose(verbose, 'warn') and compress not in {'gzip'}:
            say('warn', None, verbose,
                f"Unrecognized compression method {compress=},",
                "Will compress with gzip instead.",
            )

        # do stuff
        if action in {'copy', 'Copy', 'cp', 'move', 'Move', 'mv'}:
            if is_verbose(verbose, 'note'):
                say('note', None, verbose, f"gzip-ing '{src_path}' to '{dst_path}'")
            if not dry_run:
                with open(src_path, 'rb') as src_file:
                    with gzip.open(dst_path, 'wb') as dst_file:
                        shutil.copyfileobj(src_file, dst_file)
                        #dst_file.writelines(src_file)
            #if action in {'move', 'Move', 'mv'}:
            #    if is_verbose(verbose, 'note'):
            #        say('note', None, verbose, f"Removing '{src_path}'")
            #    if not dry_run:
            #        os.remove(src_path)
            return
    elif is_verbose(verbose, 'err'):
        say('err', None, verbose, f"Unrecognized compression method {compress=}")
        return
            
    if is_verbose(verbose, 'err'):
        say('err', None, verbose, f"Unrecognized {action=}")
    return













def get_filetree(
    src_path: str,
    src_filename: None|str = None,
    gztar_list  : set[str]|list[str] = {'.git'},
    ignore_list : set[str]|list[str] = {'__pycache__', '.ipynb_checkpoints'},
    verbose     : int  = 4,
) -> None|tuple[str, dict]:
    """Recursively scan src_path and Get a dict of its tree of file structures.

    Parameters
    ----------
    src_path: str
        File path to source file / folder
        No need to include '/' at the end. If you include it, it will be removed.
        
    src_filename: str
        File name of the source file / folder
        i.e. if src_path == "/home/admin/abc/edf/", then src_filename would be "edf"
        If None, will infer from src_path
        if src_path == "../..", then src_filename would be inferred to be ".."
            in which case, you might want to manually set it to be whatever you prefer
        
    gztar_list: list
        Skip the content inside any folder with matching names,
        i.e. only the folder is listed in the tree,
        So it's marked for archives.
        Will not do anything if it is a file.

    ignore_list: list
        Ignore files/folders within this list at all.
        Only check this if src_path points to a folder.

    verbose: int
        Wehther errors, warnings, notes, and debug info should be printed on screen. 

    Returns: src_filename, filetree
    -------
    filetree: dict
        Keywords:
            'type': str
                'dir', 'file', or 'link'
            'size': int
            'compr_mth': str    # compression method ('' for not compressing)
            'mtime_utc': int
            'sub_files': dict
                Only exist if 'type'=='dir'
                same format as this dict
    """
    # normalize path
    src_path = os.path.normpath(src_path)
    if src_filename is None:
        src_filename = os.path.basename(src_path)

    if src_filename in ignore_list:
        return None
        
    # safety check: if file exists
    #     lexist() because we want to backup symbolic links as well
    if not os.path.lexists(src_path):
        if is_verbose(verbose, 'err'):
            say('err', None, verbose, f"File '{src_path}' does not exist.")
        return None

    
    ans = {
        'type' : '',
        #'name' : '',    # not used
        'no_f' : 1,     # no of files in the directory / file
        'size' : 0,
        'compr_mth': '',    # str for compression method ('' for not compressing)
        'mtime_px6': 0,     # int for POSIX time *1e6
        'mtime_utc': '',
        #'sub_files': None,
    }


    if os.path.isfile(src_path) or os.path.islink(src_path):
        if is_verbose(verbose, 'warn') and os.path.islink(src_path):
            say('warn', None, verbose,
                f"Will not backup content in the folder pointed by symbolic link '{src_path}'")
                
        try:
            # testing if we have read permission
            with open(src_path, 'rb'):
                pass
        except PermissionError:
        #if not os.access(src_path, os.R_OK):
            if is_verbose(verbose, 'err'):
                say('err', None, verbose, f"\tPermission Error on file '{src_path}': No read access. Skipping this.")
            return None
        else:
            ans['type'] = 'file' if os.path.isfile(src_path) else 'link'
            #ans['name'] = src_filename
            ans_stat = os.stat(src_path)
            ans['size'] = ans_stat.st_size #os.path.getsize(src_path)
            ans['compr_mth'] = 'gzip'
            ans['mtime_px6'] = _get_timestamp_px6(ans_stat.st_mtime)
            #ans['mtime_utc'] = _get_timestamp_str(ans_stat.st_mtime)

    elif os.path.isdir(src_path):

        ans['type'] = 'dir'
        #ans['name'] = src_filename
        ans['sub_files'] = {}
        if src_filename not in gztar_list:
            sub_files_list   = [
                get_filetree(
                    f'{src_path}{sep}{filename}', filename,
                    gztar_list=gztar_list, ignore_list=ignore_list)
                for filename in os.listdir(src_path)
                if filename not in ignore_list
            ]
            # remove invalid files
            #ans['sub_files'] = [sub_file for sub_file in sub_files_list if sub_file is not None]
            ans['sub_files'] = {sub_file[0]: sub_file[1] for sub_file in sub_files_list if sub_file is not None}
            ans_stat = os.stat(src_path)
            ans['no_f']      = int(ans['no_f'] + np.sum([
                ans['sub_files'][sub_filename]['no_f'] for sub_filename in ans['sub_files'].keys()
            ]))
            ans['size']      = ans_stat.st_size + int(np.sum([
                ans['sub_files'][sub_filename]['size'] for sub_filename in ans['sub_files'].keys()
            ]))
            ans['compr_mth'] = ''
            ans['mtime_px6'] = int(max(_get_timestamp_px6(ans_stat.st_mtime), np.max([
                ans['sub_files'][sub_filename]['mtime_px6'] for sub_filename in ans['sub_files'].keys()
            ], initial=0)))
        else:
            ans['compr_mth'] = 'gztar'
            data = _get_dir_metadata(src_path)
            ans['size']  = data['size']
            ans['mtime_px6'] = _get_timestamp_px6(data['mtime'])
            
    ans['mtime_utc'] = _get_timestamp_str_from_px6(ans['mtime_px6'])
    
    filetree = ans
    return src_filename, filetree





def _backup_sub(
    src_path    : str,
    dst_path    : str,
    new_filetree: dict,
    old_filetree: dict,
    filecmp_shallow : bool,
    dry_run     : bool,
    verbose     : int,
) -> tuple[int, int, int, int]:
    """Recursive sub process for the backup function.
    
    Will compress and save everything to new destination.

    Returns: no_skip, no_copy, no_tgz, no_dir
    """

    no_skip = 0
    no_copy = 0
    no_tgz  = 0  # no of tgz file
    no_dir  = 0
    
    for fname in new_filetree.keys():
        new_filedata      = new_filetree[fname]
        src_filepath      = f'{src_path}{sep}{fname}'
        dst_filepath_base = f'{dst_path}{sep}{fname}'
        old_filedata = old_filetree[fname] if fname in old_filetree.keys() and isinstance(old_filetree[fname], dict) else {}
        # sanity check
        if not {'type', 'no_f', 'size', 'compr_mth', 'mtime_px6', 'mtime_utc'}.issubset(new_filedata.keys()):
            if is_verbose(verbose, 'fatal'):
                raise ValueError(
                    f"filetree corruption:"+
                    f"'type', 'no_f', 'size', 'compr_mth', 'mtime_px6', 'mtime_utc'"+
                    f"should be in {new_filedata.keys()=} but it's not.")
            do_backup = False
        # decide if the file has already been backed up
        else:
            do_backup = True
            if old_filedata:
                
                if not filecmp_shallow and is_verbose(verbose, 'fatal'):
                    raise NotImplementedError("filecmp_shallow=True has not yet been implemented.")
                    
                if not {'type', 'size', 'mtime_px6'}.issubset(old_filedata.keys()):
                    if is_verbose(verbose, 'err'):
                        say('err', None, verbose,
                            f"filetree corruption: 'type', 'size', 'mtime_px6' should be in {old_filedata.keys()=}",
                            "but it's not.")
                elif (new_filedata['mtime_px6'] == old_filedata['mtime_px6']
                      and new_filedata['size' ] == old_filedata['size']
                      and new_filedata['type' ] == old_filedata['type']):
                    do_backup = False
                    
        # do backup
        if not do_backup:
            if is_verbose(verbose, 'info'):
                say('info', None, verbose, f"Skipping {new_filedata['type']} '{src_filepath}'")
            no_skip += new_filedata['no_f']
        else:
            if new_filedata['type'] in {'file', 'link'}:
                dst_filepath = _get_bkp_filename(
                    f'{dst_filepath_base}', new_filedata['mtime_utc'], compress=new_filedata['compr_mth'], verbose=verbose)
                if is_verbose(verbose, 'warn') and os.path.exists(f'{dst_filepath}'):
                    say('warn', None, verbose,
                        f"File '{dst_filepath}' already exists- will overwrite. This should NOT have happened.")
                    
                _save_bkp_file(src_filepath, dst_filepath, action='copy', dry_run=dry_run, compress=new_filedata['compr_mth'], verbose=verbose)
                no_copy += new_filedata['no_f']
                
            elif new_filedata['type'] in {'dir'}:
                
                if new_filedata['compr_mth'] in {'gztar'}:
                    # archive the entire dir
                    
                    dst_filepath_noext = _get_bkp_filename(
                        f'{dst_filepath_base}', new_filedata['mtime_utc'], compress=False, verbose=verbose)
                    if is_verbose(verbose, 'warn') and os.path.exists(f'{dst_filepath_noext}.tar.gz'):
                        say('warn', None, verbose,
                            f"File '{dst_filepath_noext}.tar.gz' already exists- will overwrite. This should NOT have happened.")
                        
                    if is_verbose(verbose, 'note'):
                        say('note', None, verbose, f"Archiving folder '{src_filepath}' to '{dst_filepath_noext}.tar.gz'")
                    if not dry_run:
                        shutil.make_archive(dst_filepath_noext, format='gztar', root_dir=src_path, base_dir=fname)
                    no_tgz  += 1
                        
                else:
                    # go to deeper level
                    
                    dst_filepath = dst_filepath_base
                    
                    # create dst dir if non-existent
                    if not (os.path.exists(dst_filepath) and os.path.isdir(dst_filepath)):
                        if is_verbose(verbose, 'note'):
                            say('note', None, verbose, f"Creating Directory '{dst_filepath}'")
                        if not dry_run:
                            os.makedirs(dst_filepath)
                    if fname in old_filetree.keys():
                        old_filedata = old_filetree[fname]
                
                    new_no_skip, new_no_copy, new_no_tgz, new_no_dir = _backup_sub(
                        src_filepath, dst_filepath,
                        new_filetree = new_filedata['sub_files'],
                        old_filetree = old_filedata['sub_files'] if 'sub_files' in old_filedata.keys() else {},
                        filecmp_shallow = filecmp_shallow,
                        dry_run = dry_run,
                        verbose = verbose,
                    )
                    no_skip += new_no_skip
                    no_copy += new_no_copy
                    no_tgz  += new_no_tgz
                    no_dir  += new_no_dir
                    no_dir  += 1
    return no_skip, no_copy, no_tgz, no_dir



def backup(
    src_path    : str,
    dst_path    : str,
    src_filename: None|str = None,
    filecmp_shallow : bool = True,
    gztar_list  : set[str]|list[str] = {'.git'},
    ignore_list : set[str]|list[str] = {'__pycache__', '.ipynb_checkpoints'},
    dry_run     : bool = False,
    log_lvl     : bool|int = logging.DEBUG,
    verbose     : int  = 4,
) -> dict:
    """Backup data from src to dst.

    New backup function!

    WARNING: SYMBOLIC LINKS WON'T BE FOLLOWED.
    WARNING: DO NOT HAVE FILES HAVING THE SAME NAME WITH THE DIRECTORIES- WILL CAUSE CONFUSION FOR THE CODE
    WARNING: DO NOT PUT DESTINATION PATH dst_path IN ANY SUBFOLDER OF src_path- MAY CAUSE CATASTROPHIC CONSEQUENCES.
    
    
    Parameters
    ----------
    src_path: str
        Path to the source files. Could point to one file or one directory.

    dst_path: str
        Path to the backup destination where files will be stored. Could point to one file or one directory.
        
    src_filename: str
        File name of the source file / folder
        i.e. if src_path == "/home/admin/abc/edf/", then src_filename would be "edf"
        If None, will infer from src_path
        if src_path == "../..", then src_filename would be inferred to be ".."
            in which case, you might want to manually set it to be whatever you prefer
            
    filecmp_shallow: bool
        If True, will not compare src files and dst files (if exist) byte by byte;
            They will be considered true if they have the same size and modification time.

    gztar_list: list
        make an archive for folder names matching this list.

    ignore_list: list
        Do not backup files/folders within this list at all.
        Only check this if src_path points to a folder.

    dry_run: bool
        Print what will be done (if verbose >= 3) instead of actually doing.

    log_lvl : bool|int
        If true, will auto log to files.

    verbose: int
        Wehther errors, warnings, notes, and debug info should be printed on screen. 

    Returns
    -------
    filetree: dict
        See get_filetree() for format.
    """

    # normalize path
    src_path = os.path.normpath(src_path)
    dst_path = os.path.normpath(dst_path)
    if src_filename is None:
        src_filename = os.path.basename(src_path)
    dst_filepath = f'{dst_path}{sep}{src_filename}'
    metadata = {}

    
    top_timestamp_str = _get_timestamp_str(time.time())
    if log_lvl:
        if isinstance(verbose, int):
            # create backup dir if not existing
            bkp_meta_dirpath = f"{dst_path}/_bkp_meta_"
            if not (os.path.exists(bkp_meta_dirpath) and os.path.isdir(bkp_meta_dirpath)):
                if is_verbose(verbose, 'warn'):
                    say('warn', None, verbose, f"REGARDLESS OF {dry_run}, Creating Directory '{bkp_meta_dirpath}'")
                os.makedirs(bkp_meta_dirpath)
            # add auto logging
            log_filename = f"{dst_path}/_bkp_meta_/{src_filename}.filetree.bkp{top_timestamp_str}.log"
            with open(log_filename, 'a') as f:
                pass
            logging.basicConfig(filename=log_filename, level=logging.DEBUG)
            verbose = (verbose, (None, logging.getLogger(__name__)))
            if is_verbose(verbose, 'note'):
                say('note', None, verbose,
                    f"Logging to '{log_filename}',",
                    f"under {__name__=}.",
                )
        else:
            raise TypeError(f"{type(verbose)= } should be int")
    

    if is_verbose(verbose, 'note'):
        python_time_start = datetime.utcnow()
        say('note', None, verbose,
            "\n\n",
            f"Beginning backup ({dry_run=})",
            "\n",
            f"{src_path=}",
            f"{dst_filepath=}",
            "\n",
            f"Start: {python_time_start.isoformat()}",
            "\n",
            "Scanning file tree...\t",
        )

    
    # scan the folder/file to get the filetree
    ans = get_filetree(src_path, src_filename=src_filename, gztar_list=gztar_list, ignore_list=ignore_list)
    new_filetree = {ans[0]: ans[1]}

    no_files_total = ans[1]['no_f']
    say('note', None, verbose, f"Scanned {no_files_total} files.")


    # save new filetree
    new_filetree_filename    = f"{dst_path}/_bkp_meta_/{src_filename}.filetree.bkp{top_timestamp_str}.json"
    say('note', None, verbose, f"Writing file tree data to '{new_filetree_filename}'")
    if not dry_run:
        with open(new_filetree_filename, 'w') as f:
            json_dump(new_filetree, f, metadata)

    
    # read old filetree
    latest_filetree_filename = f"{dst_path}/_bkp_meta_/{src_filename}.filetree.json"
    if is_verbose(verbose, 'note'):
        say('note', None, verbose,
            f"Reading file tree data from '{latest_filetree_filename}'",
            f"Note that you can delete that file to force the code to re-backup everything.",
        )
    try:
        with open(latest_filetree_filename, 'r') as f:
            old_filetree = json_load(f)
    except JSONDecodeError:
        old_filetree = {}
        say('err', None, verbose, "Corrupted old filetree data. Will ignore old file tree and backup EVERYTHING.")
    except FileNotFoundError:
        old_filetree = {}
        say('warn', None, verbose, "No old filetree data found. Will create one.")
    else:
        say('note', None, verbose, f"Read old filetree data from '{latest_filetree_filename}'.")
    

    # normalize filetrees for backup comparison
    new_filetree_dict = new_filetree[src_filename]['sub_files']
    old_filetree_dict = {}
    if src_filename in old_filetree.keys() and 'sub_files' in old_filetree[src_filename].keys():
        old_filetree_dict = old_filetree[src_filename]['sub_files']
    #elif len(old_filetree.keys()) == 1:
    #    key = [k for k in old_filetree.keys()][0]
    #    if 'sub_files' in old_filetree[key].keys():
    #        old_filetree_dict = old_filetree[key]['sub_files']
    elif is_verbose(verbose, 'warn'):
        say('warn', None, verbose, "No valid old filetree data found. Will backup EVERYTHING.")


    # create backup dir if not existing
    if not (os.path.exists(dst_filepath) and os.path.isdir(dst_filepath)):
        say('note', None, verbose, f"Creating Directory '{dst_filepath}'")
        if not dry_run:
            os.makedirs(dst_filepath)

    if is_verbose(verbose, 'note'):
        say('note', None, verbose, "Filetree read complete.")
        python_time_ended = datetime.utcnow()
        python_time__used  = python_time_ended - python_time_start
        say('note', None, verbose, "\n", f"Now  : {python_time_ended.isoformat()}", f"Time Used: {python_time__used}\n")
        say('note', None, verbose, f"\n\n\tBeginning backup...\n\n")
        
    
    # do backup
    no_skip, no_copy, no_tgz, no_dir = _backup_sub(
        src_path, dst_filepath,
        new_filetree = new_filetree_dict,
        old_filetree = old_filetree_dict,
        filecmp_shallow = filecmp_shallow,
        dry_run = dry_run,
        verbose = verbose,
    )
    no_dir += 1 # counting itself
    say('note', None, verbose,
        "\n",
        f"Skipped  {no_skip} files,",
        f"Copied   {no_copy} files,",
        f"Archived {no_tgz } directories,",
        f"Entered  {no_dir } directories,",
        f"Totally processed {no_skip+no_copy+no_tgz+no_dir}  / {no_files_total} files.",
        "\n",
    )
    

    # update filetree
    if not dry_run:
        say('note', None, verbose, f"Overwriting file tree data from '{new_filetree_filename}' to '{latest_filetree_filename}'")
        shutil.copy2(new_filetree_filename, latest_filetree_filename)
        

    # record time used
    if is_verbose(verbose, 'note'):
        python_time_ended = datetime.utcnow()
        python_time__used  = python_time_ended - python_time_start
        say('note', None, verbose, "\n", f"Ended: {python_time_ended.isoformat()}", f"Time Used: {python_time__used}\n")
        say('note', None, verbose, f"\n\n\n\t\t--- All done ---\n\n\n")

    return new_filetree
