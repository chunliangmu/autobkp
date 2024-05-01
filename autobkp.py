"""A script to auto-backup data.

Author: Chunliang Mu
"""


from .log import say, is_verbose
from .readwrite import json_dump, json_load

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
    return datetime.utcfromtimestamp(timestamp).strftime("%Y%m%d%H%M%S")



def _get_bkp_filename_format(dst_path: str, dst_mtime: float) -> str:
    """f-string combine dst path and mtime into backup file name.
    """
    dst_path = os.path.normpath(dst_path)
    dst_mtimestamp = _get_timestamp_str(min(dst_mtime, time.time()))
    dst_path_new = f'{dst_path}.bkp{dst_mtimestamp}._bkp_'
    return dst_path_new



def _get_dir_mtime(src_path: str) -> float:
    """Recursively get the newest mtime for a dir.

    dst_path: str
        path to a file. Must not end with '/'. (Does not check that)
    
    """
    mtime = os.path.getmtime(src_path)
    if os.path.isdir(src_path):
        for filename in os.listdir(src_path):
            src_path_new = f'{src_path}{sep}{filename}'
            if os.path.isdir(src_path_new):
                new_mtime = _get_dir_mtime(src_path_new)
            else:
                new_mtime = os.path.getmtime(src_path_new)
            if new_mtime > mtime:
                mtime = new_mtime
    return mtime



def _get_dir_metadata(src_path: str) -> float:
    """Recursively get the metadata (newest mtime & total size) for a dir.

    Ignores things in symbolic links.

    dst_path: str
        path to a file. Must not end with '/'. (Does not check that)
    
    """
    data = {
        'size' : os.path.getsize( src_path),
        'mtime': os.path.getmtime(src_path),
    }
    if os.path.isdir(src_path):
        for filename in os.listdir(src_path):
            src_path_new = f'{src_path}{sep}{filename}'
            new_data = _get_dir_metadata(src_path_new)
            data['size'] += new_data['size']
            if new_data['mtime'] > data['mtime']:
                data['mtime'] = new_data['mtime']
    return data



def _get_bkp_filename(dst_path: str, compress: str = False) -> str:
    """Get backup file path from src file path.

    dst_path: str
        path to a file. Must not end with '/'.
    
    """
    dst_path = os.path.normpath(dst_path)
    dst_mtime = _get_dir_mtime(dst_path)
    dst_path_new = _get_bkp_filename_format(dst_path, dst_mtime)
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
            #    if iverbose >= 3:
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
    gztar_list : list = ['.git'],
    ignore_list: list = ['__pycache__', '.ipynb_checkpoints'],
    dry_run  : bool = False,
    top_level: bool = True,
    iverbose :  int = 4,
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
        Print what will be done (if iverbose >= 3) instead of actually doing.

    bkp_old_dst_files: bool
        Whether or not to backup existing destination files if it is older.
        If == 'gzip', will compress the file while saving.

    gztar_list: list
        make an archive for folder names matching this list.

    ignore_list: list
        Do not backup files/folders within this list at all.
        Only check this if src_path points to a folder.

    iverbose: int
        Wehther errors, warnings, notes, and debug info should be printed on screen. 

    Returns
    -------
    no_file_checked, no_file_changed
    no_src_peeked: int
        No of source files checked by this func
    no_src_backed: int
        No of source files backed up (i.e. copied) by this func
    """

    # init
    no_src_peeked = 0
    no_src_backed = 0
    # normalize path
    src_path = os.path.normpath(src_path)
    dst_path = os.path.normpath(dst_path)
    

    # safety check: if file exists
    #     lexist() because we want to backup symbolic links as well
    if not os.path.lexists(src_path):
        if is_verbose(iverbose, 'err'):
            say('err', 'dir_backup()', iverbose, f"File '{src_path}' does not exist.")
        return no_src_peeked, no_src_backed
    
    no_src_peeked += 1
    if os.path.isfile(src_path) or os.path.islink(src_path):
        if os.path.islink(src_path):
            # warn
            if iverbose >= 2:
                print(f"**  Warning: dir_backup(...):\n" + \
                      f"\tWill not backup content in the folder pointed by symbolic link '{src_path}'.")
                
        try:
            with open(src_path, 'rb'):
                pass
        except PermissionError:
            if iverbose:
                print(f"*** Error: dir_backup(...):\n" + \
                      f"\tPermission Error on file '{dst_path}'.")
        else:
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
                        elif bkp_old_dst_files:
                            _save_bkp_file(dst_path, dst_path_new, 'copy', dry_run, bkp_old_dst_files, iverbose)
                            no_src_backed += 1
                elif bkp_old_dst_files:
                    dst_path_new = _get_bkp_filename(dst_path, bkp_old_dst_files)
                    _save_bkp_file(dst_path, dst_path_new, 'move', dry_run, bkp_old_dst_files, iverbose)

            # now copy
            if do_copy:
                try:
                    _save_bkp_file(src_path, dst_path, 'copy', dry_run, False, iverbose)
                    no_src_backed += 1
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
                        no_src_backed += 1
                

    elif os.path.isdir(src_path):
        # create dst dir if non-existent
        if not os.path.exists(dst_path):
            if iverbose:
                print(f"*   Note:\tCreating Directory '{dst_path}'")
            if not dry_run:
                os.makedirs(dst_path)
        
        for filename in os.listdir(src_path):
            if filename not in ignore_list:
                src_path_new = f'{src_path}{sep}{filename}'
                dst_path_new = f'{dst_path}{sep}{filename}'
                if (iverbose >= 3 and top_level) or (iverbose >= 4 and os.path.isdir(src_path_new)):
                    print(f"\nWorking on sub-folder {src_path_new}...")
                    if top_level:
                        print(f"({no_src_peeked} files looked, {no_src_backed} files backed up so far.\n)")
                    else:
                        print()
                if filename in gztar_list:
                    # archive the entire dir
                    dst_path_new_bkp = _get_bkp_filename_format(dst_path_new, _get_dir_mtime(src_path_new))
                    if not os.path.exists(f'{dst_path_new_bkp}.tar.gz'):
                        if iverbose:
                            print(
                                f"*   Note:\tMaking archive of folder '{filename}' in path '{src_path}'",
                                f"at file '{dst_path_new_bkp}'.tar.gz")
                        if not dry_run:
                            shutil.make_archive(dst_path_new_bkp, format='gztar', root_dir=src_path, base_dir=filename)
                else:
                    # backup files one by one
                    new_src_peeked, new_src_backed = dir_backup(
                        src_path_new,
                        dst_path_new,
                        filecmp_shallow   = filecmp_shallow,
                        bkp_old_dst_files = bkp_old_dst_files if filename not in gztar_list else False, 
                        gztar_list=gztar_list,
                        ignore_list=ignore_list,
                        top_level = False,
                        dry_run=dry_run, iverbose=iverbose,
                        )
                    no_src_peeked += new_src_peeked
                    no_src_backed += new_src_backed
    return no_src_peeked, no_src_backed














def get_file_tree(
    src_path: str,
    src_filename: str|None = None,
    gztar_list  : set|list = {'.git'},
    ignore_list : set|list = {'__pycache__', '.ipynb_checkpoints'},
) -> tuple[str, dict]|None:
    """Scan src_path and Get a dict of its tree of file structures.

    Parameters
    ----------
    src_path: str
        File path to source file / folder
        No need to include '/' at the end. If you include it, it will be removed.
        
    src_filename: str
        File name of the source file / folder
        i.e. if src_path == "/home/admin/abc/edf/", then src_filename would be "edf"
        If None, will infer from src_path
        
    gztar_list: list
        Skip the content inside any folder with matching names,
        i.e. only the folder is listed in the tree,
        So it's marked for archives.
        Will not do anything if it is a file.

    ignore_list: list
        Ignore files/folders within this list at all.
        Only check this if src_path points to a folder.

    Returns
    -------
    filetree: dict
        Keywords:
            'type': str
                'dir', 'file', or 'link'
            'size': int
            'gztar': bool|str (str for filename filetype type suffix)
            'mtime': float
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
        if is_verbose(iverbose, 'err'):
            say('err', 'get_file_tree()', iverbose, f"File '{src_path}' does not exist.")
        return None

    
    ans = {
        'type' : '',
        #'name' : '',
        'size' : 0,
        'gztar': False,    # str for filename filetype type suffix
        'mtime': 0.,
        'mtime_utc': '',
        #'sub_files': None,
    }


    if os.path.isfile(src_path) or os.path.islink(src_path):
        if os.path.islink(src_path):
            # warn
            if is_verbose(iverbose, 'warn'):
                say('warn', 'get_file_tree()', iverbose,
                    f"Will not backup content in the folder pointed by symbolic link '{src_path}'")
                
        try:
            # testing if we have read permission
            with open(src_path, 'rb'):
                pass
        except PermissionError:
            if is_verbose(iverbose, 'err'):
                say('err', 'get_file_tree()', iverbose, f"\tPermission Error on file '{src_path}'. Skipping this.")
            return None
        else:
            ans['type'] = 'file' if os.path.isfile(src_path) else 'link'
            #ans['name'] = src_filename
            ans['size'] = os.path.getsize(src_path)
            ans['mtime'] = os.path.getmtime(src_path)

    elif os.path.isdir(src_path):

        ans['type'] = 'dir'
        #ans['name'] = src_filename
        ans['sub_files'] = []
        if src_filename not in gztar_list:
            sub_files_list   = [
                get_file_tree(
                    f'{src_path}{sep}{filename}', filename,
                    gztar_list=gztar_list, ignore_list=ignore_list)
                for filename in os.listdir(src_path)
                if filename not in ignore_list
            ]
            # remove invalid files
            #ans['sub_files'] = [sub_file for sub_file in sub_files_list if sub_file is not None]
            ans['sub_files'] = {sub_file[0]: sub_file[1] for sub_file in sub_files_list if sub_file is not None}
            ans['size']      = os.path.getsize(src_path) + int(np.sum([
                ans['sub_files'][sub_filename]['size'] for sub_filename in ans['sub_files'].keys()
            ]))
            ans['mtime']     = float(max(os.path.getmtime(src_path), np.max([
                ans['sub_files'][sub_filename]['mtime'] for sub_filename in ans['sub_files'].keys()
            ])))
        else:
            ans['gztar'] = True
            data = _get_dir_metadata(src_path)
            ans['size']  = data['size']
            ans['mtime'] = data['mtime']
            
    ans['mtime_utc'] = _get_timestamp_str(ans['mtime'])
    return src_filename, ans





def backup(
    src_path: str,
    dst_path: str,
    filecmp_shallow: bool = True,
    gztar_list  : set|list = {'.git'},
    ignore_list : set|list = {'__pycache__', '.ipynb_checkpoints'},
    dry_run     : bool = False,
    top_level   : bool = True,
    iverbose    :  int = 4,
):
    """Backup data from src to dst.

    New backup function!

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
        Print what will be done (if iverbose >= 3) instead of actually doing.

    bkp_old_dst_files: bool
        Whether or not to backup existing destination files if it is older.
        If == 'gzip', will compress the file while saving.

    gztar_list: list
        make an archive for folder names matching this list.

    ignore_list: list
        Do not backup files/folders within this list at all.
        Only check this if src_path points to a folder.

    iverbose: int
        Wehther errors, warnings, notes, and debug info should be printed on screen. 

    Returns
    -------
    no_file_checked, no_file_changed
    no_src_peeked: int
        No of source files checked by this func
    no_src_backed: int
        No of source files backed up (i.e. copied) by this func
    """
    
    raise NotImplementedError

    # normalize path
    src_path = os.path.normpath(src_path)
    dst_path = os.path.normpath(dst_path)
    src_filename = os.path.basename(src_path)

    metadata = {}

    
    ans = get_file_tree(src_path, gztar_list=gztar_list, ignore_list=ignore_list)
    new_file_tree = {ans[0]: ans[1]}

    with open(f"{dst_path}/_bkp_meta_/{src_filename}.filetree.bkp{_get_timestamp_str(time.time())}.json", 'w') as f:
        # open to-be-saved file first to make sure it is okay for saving

        
        # save data
        json_dump(new_file_tree, f, metadata)

