"""A script to auto-backup data.

Author: Chunliang Mu
"""


from .log import say, is_verbose
from .readwrite import json_dump, json_load, JSONDecodeError

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


def _get_timestamp_int(timestamp: float) -> int:
    """Get the int version of time. Returns value in utc and is semi-human-readable.
    """
    return int(_get_timestamp_str(timestamp))


def _get_timestamp_str_from_int(timestamp_utc: int, verbose:int=4) -> str:
    """Get the str version of time. Returns value in utc and is semi-human-readable.
    """
    #return datetime.utcfromtimestamp(timestamp).strftime("%Y%m%d%H%M%S")
    if isinstance(timestamp_utc, int):
        return f"{timestamp_utc:014d}"
    else:
        if is_verbose(verbose, 'fatal'):
            raise TypeError(
                "input timestamp should be of int type. Did you meant to use _get_timestamp_str() instead of _get_timestamp_str_from_int()?")



if False:
    def _get_bkp_filename_format(dst_path: str, dst_mtime: float) -> str:
        """f-string combine dst path and mtime into backup file name.
        """
        dst_path = os.path.normpath(dst_path)
        dst_mtimestamp = _get_timestamp_str(min(dst_mtime, time.time()))
        dst_path_new = f'{dst_path}.bkp{dst_mtimestamp}._bkp_'
        return dst_path_new

def _get_bkp_filename(dst_path: str, mtime_utc: int, compress: bool|str = True, verbose: int=4) -> str:
    """f-string combine dst path and mtime into backup file name.
    """
    dst_path = os.path.normpath(dst_path)
    dst_path_new = f'{dst_path}.bkp{_get_timestamp_str_from_int(mtime_utc, verbose)}._bkp_'
    if compress:
        dst_path_new += '.gz'
    return dst_path_new



if False:
    def _get_dir_mtime(src_path: str) -> float:
        """Recursively get the newest mtime for a dir.
    
        dst_path: str
            path to a file. Must not end with '/'. (Does not check that)
        
        """
        mtime = os.path.getmtime(src_path)
        if os.path.isdir(src_path):
            for filename in os.listdir(src_path):
                new_mtime = _get_dir_mtime(f'{src_path}{sep}{filename}')
                if new_mtime > mtime:
                    mtime = new_mtime
        return mtime



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




        
if False:
    # commented
    def _get_bkp_filename(dst_path: str, compress: bool|str = False) -> str:
        """Get backup file path from src file path.
    
        dst_path: str
            path to a file. Must not end with '/'.
        
        """
        dst_path = os.path.normpath(dst_path)
        dst_mtime = _get_dir_mtime(dst_path)
        dst_path_new = _get_bkp_filename_format(dst_path, dst_mtime)
        if compress:# == 'gzip':
            dst_path_new = f'{dst_path_new}.gz'
        return dst_path_new


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
    if compress:# == 'gzip':
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
    else:
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
    if is_verbose(verbose, 'err'):
        say('err', None, verbose, f"Unrecognized {action=}")
    return



if False:
    def dir_backup(
        src_path: str,
        dst_path: str,
        filecmp_shallow: bool = True,
        bkp_old_dst_files: bool | str = 'gzip',
        gztar_list : list = ['.git'],
        ignore_list: list = ['__pycache__', '.ipynb_checkpoints'],
        dry_run  : bool = False,
        top_level: bool = True,
        verbose  :  int = 4,
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
            Print what will be done (if verbose >= 3) instead of actually doing.
    
        bkp_old_dst_files: bool
            Whether or not to backup existing destination files if it is older.
            If == 'gzip', will compress the file while saving.
    
        gztar_list: list
            make an archive for folder names matching this list.
    
        ignore_list: list
            Do not backup files/folders within this list at all.
            Only check this if src_path points to a folder.
    
        verbose: int
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
            if is_verbose(verbose, 'err'):
                say('err', 'dir_backup()', verbose, f"File '{src_path}' does not exist.")
            return no_src_peeked, no_src_backed
        
        no_src_peeked += 1
        if os.path.isfile(src_path) or os.path.islink(src_path):
            if os.path.islink(src_path):
                # warn
                if verbose >= 2:
                    print(f"**  Warning: dir_backup(...):\n" + \
                          f"\tWill not backup content in the folder pointed by symbolic link '{src_path}'.")
                    
            try:
                with open(src_path, 'rb'):
                    pass
            except PermissionError:
                if verbose:
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
                                _save_bkp_file(dst_path, dst_path_new, 'copy', dry_run, bkp_old_dst_files, verbose)
                                no_src_backed += 1
                    elif bkp_old_dst_files:
                        dst_path_new = _get_bkp_filename(dst_path, bkp_old_dst_files)
                        _save_bkp_file(dst_path, dst_path_new, 'move', dry_run, bkp_old_dst_files, verbose)
    
                # now copy
                if do_copy:
                    try:
                        _save_bkp_file(src_path, dst_path, 'copy', dry_run, False, verbose)
                        no_src_backed += 1
                    except FileNotFoundError:
                        if verbose:
                            print(f"*** Error: dir_backup(...):\n" + \
                          f"\tCannot copy to '{dst_path}'.")
                    except shutil.SameFileError:
                        dst_path_new = _get_bkp_filename(src_path, bkp_old_dst_files)
                        #     check if backup file already existed
                        if os.path.lexists(dst_path_new) and filecmp.cmp(src_path, dst_path_new):
                            pass
                        else:
                            _save_bkp_file(src_path, dst_path_new, 'copy', dry_run, bkp_old_dst_files, verbose)
                            no_src_backed += 1
                    
    
        elif os.path.isdir(src_path):
            # create dst dir if non-existent
            if not os.path.exists(dst_path):
                if verbose:
                    print(f"*   Note:\tCreating Directory '{dst_path}'")
                if not dry_run:
                    os.makedirs(dst_path)
            
            for filename in os.listdir(src_path):
                if filename not in ignore_list:
                    src_path_new = f'{src_path}{sep}{filename}'
                    dst_path_new = f'{dst_path}{sep}{filename}'
                    if (verbose >= 3 and top_level) or (verbose >= 4 and os.path.isdir(src_path_new)):
                        print(f"\nWorking on sub-folder {src_path_new}...")
                        if top_level:
                            print(f"({no_src_peeked} files looked, {no_src_backed} files backed up so far.\n)")
                        else:
                            print()
                    if filename in gztar_list:
                        # archive the entire dir
                        dst_path_new_bkp = _get_bkp_filename_format(dst_path_new, _get_dir_mtime(src_path_new))
                        if not os.path.exists(f'{dst_path_new_bkp}.tar.gz'):
                            if verbose:
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
                            dry_run=dry_run, verbose=verbose,
                            )
                        no_src_peeked += new_src_peeked
                        no_src_backed += new_src_backed
        return no_src_peeked, no_src_backed














def get_filetree(
    src_path: str,
    src_filename: None|str = None,
    gztar_list  : set[str]|list[str] = {'.git'},
    ignore_list : set[str]|list[str] = {'__pycache__', '.ipynb_checkpoints'},
    verbose     : int  = 4,
) -> None|tuple[str, dict]:
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
            'use_gztar': bool|str (str for filename filetype type suffix)
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
        'size' : 0,
        'use_gztar': False,    # str for filename filetype type suffix
        #'mtime' : 0.,   # not used
        'mtime_utc': 0,
        #'sub_files': None,
    }


    if os.path.isfile(src_path) or os.path.islink(src_path):
        if os.path.islink(src_path):
            # warn
            if is_verbose(verbose, 'warn'):
                say('warn', None, verbose,
                    f"Will not backup content in the folder pointed by symbolic link '{src_path}'")
                
        #try:
        #    # testing if we have read permission
        #    with open(src_path, 'rb'):
        #        pass
        #except PermissionError:
        if not os.access(src_path, os.R_OK):
            if is_verbose(verbose, 'err'):
                say('err', None, verbose, f"\tPermission Error on file '{src_path}': No read access. Skipping this.")
            return None
        else:
            ans['type'] = 'file' if os.path.isfile(src_path) else 'link'
            #ans['name'] = src_filename
            ans['size'] = os.path.getsize(src_path)
            #ans['mtime'] = os.path.getmtime(src_path)
            ans['mtime_utc'] = _get_timestamp_int(os.path.getmtime(src_path))

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
            ans['size']      = os.path.getsize(src_path) + int(np.sum([
                ans['sub_files'][sub_filename]['size'] for sub_filename in ans['sub_files'].keys()
            ]))
            ans['mtime_utc'] = int(max(_get_timestamp_int(os.path.getmtime(src_path)), np.max([
                ans['sub_files'][sub_filename]['mtime_utc'] for sub_filename in ans['sub_files'].keys()
            ])))
        else:
            ans['use_gztar'] = True
            data = _get_dir_metadata(src_path)
            ans['size']  = data['size']
            ans['mtime_utc'] = _get_timestamp_int(data['mtime'])
            
    #ans['mtime_utc'] = _get_timestamp_int(ans['mtime'])
    
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
):
    """Sub process for the backup function.
    
    Will compress and save everything to new destination.
    """

    COMPRESS = 'gzip'
    compress = COMPRESS
    
    for fname in new_filetree.keys():
        new_filedata      = new_filetree[fname]
        src_filepath      = f'{src_path}{sep}{fname}'
        dst_filepath_base = f'{dst_path}{sep}{fname}'
        old_filedata = old_filetree[fname] if fname in old_filetree.keys() and isinstance(old_filetree[fname], dict) else {}
        # sanity check
        if not {'type', 'size', 'mtime_utc', 'use_gztar'}.issubset(new_filedata.keys()):
            if is_verbose(verbose, 'fatal'):
                raise ValueError(
                    f"filetree corruption: 'type', 'size', 'mtime_utc', 'use_gztar' should be in {new_filedata.keys()=} but it's not.")
            do_backup = False
        # decide if the file has already been backed up
        else:
            do_backup = True
            if old_filedata:
                
                if not filecmp_shallow and is_verbose(verbose, 'fatal'):
                    raise NotImplementedError("filecmp_shallow=True has not yet been implemented.")
                    
                if not {'type', 'size', 'mtime_utc'}.issubset(old_filedata.keys()):
                    if is_verbose(verbose, 'err'):
                        say('err', None, verbose,
                            f"filetree corruption: 'type', 'size', 'mtime_utc' should be in {old_filedata.keys()=}",
                            "but it's not.")
                elif (new_filedata['mtime_utc'] == old_filedata['mtime_utc']
                      and new_filedata['size' ] == old_filedata['size']
                      and new_filedata['type' ] == old_filedata['type']):
                    do_backup = False
                    
        # do backup
        if not do_backup:
            if is_verbose(verbose, 'note'):
                say('note', None, verbose, f"Skipping {new_filedata['type']} '{src_filepath}'")
        else:
            if new_filedata['type'] in {'file', 'link'}:
                dst_filepath = _get_bkp_filename(
                    f'{dst_filepath_base}', new_filedata['mtime_utc'], compress=compress, verbose=verbose)
                if is_verbose(verbose, 'warn') and os.path.exists(f'{dst_filepath}'):
                    say('warn', None, verbose,
                        f"File '{dst_filepath}' already exists- will overwrite. This should NOT have happened.")
                    
                _save_bkp_file(src_filepath, dst_filepath, action='copy', dry_run=dry_run, compress=compress, verbose=verbose)
                
            elif new_filedata['type'] in {'dir'}:
                
                if new_filedata['use_gztar']:
                    # archive the entire dir
                    
                    dst_filepath_noext = _get_bkp_filename(
                        f'{dst_filepath_base}', new_filedata['mtime_utc'], compress=False, verbose=verbose)
                    if is_verbose(verbose, 'warn') and not os.path.exists(f'{dst_filepath_noext}.tar.gz'):
                        say('warn', None, verbose,
                            f"File '{dst_filepath_noext}.tar.gz' already exists- will overwrite. This should NOT have happened.")
                        
                    if is_verbose(verbose, 'note'):
                        say('note', None, verbose, f"Archiving folder '{src_filepath}' to '{dst_filepath_noext}.tar.gz'")
                    if not dry_run:
                        shutil.make_archive(dst_filepath_noext, format='gztar', root_dir=src_path, base_dir=fname)
                        
                else:
                    # go to deeper level
                    
                    dst_filepath = dst_filepath_base
                    
                    # create dst dir if non-existent
                    if not (os.path.exists(dst_filepath) and os.isdir(dst_filepath)):
                        if is_verbose(verbose, 'note'):
                            say('note', None, verbose, f"Creating Directory '{dst_filepath}'")
                        if not dry_run:
                            os.makedirs(dst_filepath)
                    if fname in old_filetree.keys():
                        old_filedata = old_filetree[fname]
                
                    _backup_sub(
                        src_filepath, dst_filepath,
                        new_filetree = new_filedata['sub_files'],
                        old_filetree = old_filedata['sub_files'] if 'sub_files' in old_filedata.keys() else {},
                        filecmp_shallow = filecmp_shallow,
                        dry_run = dry_run,
                        verbose = verbose,
                    )
    return



def backup(
    src_path    : str,
    dst_path    : str,
    src_filename: None|str = None,
    filecmp_shallow : bool = True,
    gztar_list  : set[str]|list[str] = {'.git'},
    ignore_list : set[str]|list[str] = {'__pycache__', '.ipynb_checkpoints'},
    dry_run     : bool = False,
    verbose     : int  = 4,
) -> dict:
    """Backup data from src to dst.

    New backup function!

    WARNING: SYMBOLIC LINKS WON'T BE FOLLOWED.
    WARNING: DO NOT HAVE FILES HAVING THE SAME NAME WITH THE DIRECTORIES- WILL CAUSE CONFUSION FOR THE CODE
    
    
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

    dry_run: bool
        Print what will be done (if verbose >= 3) instead of actually doing.

    bkp_old_dst_files: bool
        Whether or not to backup existing destination files if it is older.
        If == 'gzip', will compress the file while saving.

    gztar_list: list
        make an archive for folder names matching this list.

    ignore_list: list
        Do not backup files/folders within this list at all.
        Only check this if src_path points to a folder.

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
    metadata = {}

    
    # scan the folder/file to get the filetree
    ans = get_filetree(src_path, src_filename=src_filename, gztar_list=gztar_list, ignore_list=ignore_list)
    new_filetree = {ans[0]: ans[1]}


    # save new filetree
    new_filetree_filename    = f"{dst_path}/_bkp_meta_/{src_filename}.filetree.bkp{_get_timestamp_str(time.time())}.json"
    if is_verbose(verbose, 'note'):
        say('note', None, verbose, f"Writing file tree data to '{new_filetree_filename}'")
    if not dry_run:
        with open(new_filetree_filename, 'w') as f:
            json_dump(new_filetree, f, metadata)

    
    # read old filetree
    latest_filetree_filename = f"{dst_path}/_bkp_meta_/{src_filename}.filetree.json"
    try:
        with open(latest_filetree_filename, 'r') as f:
            old_filetree = json_load(f)
    except JSONDecodeError:
        old_filetree = {}
        if is_verbose(verbose, 'err'):
            say('err', None, verbose, "Corrupted old filetree data. Will ignore old file tree and backup EVERYTHING.")
    except FileNotFoundError:
        old_filetree = {}
        if is_verbose(verbose, 'warn'):
            say('warn', None, verbose, "No old filetree data found. Will create one.")
    else:
        if is_verbose(verbose, 'note'):
            say('note', None, verbose, f"Read old filetree data from '{latest_filetree_filename}'.")
    

    # do backup
    _backup_sub(
        src_filepath, dst_filepath,
        new_filetree = new_filetree,
        old_filetree = old_filetree,
        filecmp_shallow = filecmp_shallow,
        dry_run = dry_run,
        verbose = verbose,
    )
    

    # update filetree
    if is_verbose(verbose, 'note'):
        say('note', None, verbose, f"Overwriting file tree data from '{new_filetree_filename}' to '{latest_filetree_filename}'")
    if not dry_run:
        shutil.copy2(new_filetree_filename, latest_filetree_filename)

    if is_verbose(verbose, 'note'):
        say('note', None, verbose, f"\t--- All done ---")

    return new_filetree
