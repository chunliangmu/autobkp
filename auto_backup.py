#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""A script to auto-backup data.

Author: Chunliang Mu
"""

import os
import shutil
import filecmp
from datetime import datetime
import time
import gzip



def _get_bkp_filename(dst_path: str, compress: str = False) -> str:
    """Get backup file path from src file path.

    dst_path: str
        path to a file. Must not end with '/'. (Does not check that)
    
    """
    dst_mtime = os.path.getmtime(dst_path)
    dst_mtimestamp = datetime.utcfromtimestamp(min(dst_mtime, time.time())).strftime("%Y%m%d%H%M%S")
    dst_path_new = f'{dst_path}.bkp{dst_mtimestamp}._backup_'
    if compress == 'gzip':
        dst_path_new = f'{dst_path_new}.gz'
    return dst_path_new



def _save_bkp_file(
    src_path: str, dst_path: str,
    action: str = 'copy',
    dry_run: bool = False,
    compress: str = False,
    iverbose: int = 4,
):
    """Save source file to the destination file.    
    """
    if compress == 'gzip':
        if action in ['copy', 'Copy', 'cp', 'move', 'Move', 'mv']:
            if iverbose:
                print(f"*   Note:\tgzip-ing '{src_path}' to '{dst_path}'")
            if not dry_run:
                with open(src_path, 'rb') as src_file:
                    with gzip.open(dst_path, 'wb') as dst_file:
                        dst_file.writelines(src_file)
            #if action in ['move', 'Move', 'mv']:
            #    if iverbose:
            #        print(f"*   Note:\tRemoving '{src_path}'")
            #    if not dry_run:
            #        os.remove(src_path)
    else:
        if action in ['copy', 'Copy', 'cp']:
            if iverbose:
                print(f"*   Note:\tCopying '{src_path}' to '{dst_path}'")
            if not dry_run:
                shutil.copy2(src_path, dst_path, follow_symlinks=False)
        elif action in ['move', 'Move', 'mv']:
            if iverbose:
                print(f"*   Note:\tMoving '{src_path}' to '{dst_path}'")
            if not dry_run:
                shutil.copy2(src_path, dst_path)
        else:
            raise NotImplementedError
    return



def dir_backup(
    src_path: str,
    dst_path: str,
    filecmp_shallow: bool = True,
    bkp_old_dst_files: {bool, str} = 'gzip',
    bkp_old_dst_files_excl_list: list = ['.git'],
    ignore_list: list = ['__pycache__', '.ipynb_checkpoints'],
    dry_run: bool = False,
    iverbose: int = 4,
):
    """Recursively backup data from src to dst.

    WARNING: SYMBOLIC LINKS WON'T BE FOLLOWED.
    
    
    Parameters
    ----------
    src_path: str
        Path to the source files. Could point to one file or one directory.

    dst_path: str
        Path to the backup destination where files will be stored. Could point to one file or one directory.

    filecmp_shallow: bool
        If True, will not compare src files and dst files (if exist) byte by byte;

    dry_run: bool
        Print what will be done (if iverbose > 0) instead of actually doing.

    bkp_old_dst_files: bool
        Whether or not to backup existing destination files if it is older.
        If == 'gzip', will compress the file while saving.

    bkp_old_dst_files_excl_list: list
        Do not keep older version backups for files/folders within this list.

    ignore_list: list
        Do not backup files/folders within this list at all.
        Only check this if src_path points to a folder.

    iverbose: int
        Wehther errors, warnings, notes, and debug info should be printed on screen. 

    Returns
    -------
    state: int
        0 if successful, otherwise non-zero.
    """

    # init
    ans = 0
    # normalize path
    src_path = os.path.normpath(src_path)
    dst_path = os.path.normpath(dst_path)
    

    # safety check: if file exists
    #     lexist() because we want to backup symbolic links as well
    if not os.path.lexists(src_path):
        if iverbose:
            print(f"*** Error: dir_backup(...):\n" + \
                  f"\tFile '{src_path}' does not exist.")
        return -1
    
    if os.path.isfile(src_path) or os.path.islink(src_path):
        if os.path.islink(src_path):
            # warn
            if iverbose >= 2:
                print(f"**  Warning: dir_backup(...):\n" + \
                      f"\tWill not backup content in the folder pointed by symbolic link '{src_path}'.")
                
        # compare and decide if it's the same file
        do_copy = True
        if os.path.lexists(dst_path) and not os.path.isdir(dst_path):
            if filecmp.cmp(src_path, dst_path):
                # same file content...
                do_copy = False
                if os.path.samefile(src_path, dst_path):
                    # and is the same exact file! (we don't want that since we want multiple physical copy)
                    #     check if backup file already existed
                    dst_path_new = _get_bkp_filename(dst_path, bkp_old_dst_files)
                    if os.path.lexists(dst_path_new) and filecmp.cmp(dst_path, dst_path_new):
                        pass
                    else:
                        _save_bkp_file(dst_path, dst_path_new, 'copy', dry_run, bkp_old_dst_files, iverbose)
            else:
                dst_path_new = _get_bkp_filename(dst_path, bkp_old_dst_files)
                _save_bkp_file(dst_path, dst_path_new, 'move', dry_run, bkp_old_dst_files, iverbose)

        # now copy
        if do_copy:
            try:
                _save_bkp_file(src_path, dst_path, 'copy', dry_run, False, iverbose)
            except FileNotFoundError:
                if iverbose:
                    print(f"*** Error: dir_backup(...):\n" + \
                  f"\tCannot copy to '{dst_path}'.")
            except shutil.SameFileError:
                dst_path_new = _get_bkp_filename(src_path, bkp_old_dst_files)
                #     check if backup file already existed
                if os.path.lexists(dst_path_new) and filecmp.cmp(src_path, dst_path_new):
                    pass
                else:
                    _save_bkp_file(src_path, dst_path_new, 'copy', dry_run, bkp_old_dst_files, iverbose)
                

    elif os.path.isdir(src_path):
        # create dst dir if non-existent
        if not os.path.exists(dst_path):
            if iverbose:
                print(f"*   Note:\tCreating Directory '{dst_path}'")
            if not dry_run:
                os.makedirs(dst_path)
        
        for filename in os.listdir(src_path):
            if filename not in ignore_list:
                res = dir_backup(
                    f'{src_path}{os.path.sep}{filename}',
                    f'{dst_path}{os.path.sep}{filename}',
                    filecmp_shallow   = filecmp_shallow,
                    bkp_old_dst_files = bkp_old_dst_files if filename not in bkp_old_dst_files_excl_list else False, 
                    bkp_old_dst_files_excl_list=bkp_old_dst_files_excl_list,
                    ignore_list=ignore_list,
                    dry_run=dry_run, iverbose=iverbose,
                    )
                if res:
                    ans = 1
    return ans

