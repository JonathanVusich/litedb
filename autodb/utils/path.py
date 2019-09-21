from os import scandir, listdir, DirEntry, path
from typing import Dict

from ..errors import DatabaseNotFound


def load_tables(directory: str) -> str:
    """Scans the given directory for tables and returns any valid table directories."""
    tables = 0
    with scandir(path=directory) as curdir:
        for entry in curdir:
            if is_table(entry):
                tables += 1
                yield entry
    if tables == 0:
        raise DatabaseNotFound


def is_table(directory: DirEntry) -> bool:
    """Checks if a directory contains a table."""
    return directory.is_dir(follow_symlinks=False) and valid_table_contents(directory.path)


def is_shard_file(file: DirEntry) -> bool:
    """Checks if a file is a shard."""
    return is_shard(file.name) and file.is_file(follow_symlinks=False)


def is_index_file(file: DirEntry) -> bool:
    """Checks if a file is an index."""
    return is_index(file.name) and file.is_file(follow_symlinks=False)


def is_info_file(file: DirEntry) -> bool:
    """Checks if a file is an info file."""
    return is_info(file.name) and file.is_file(follow_symlinks=False)


def is_shard(file_name: str) -> bool:
    """Determines if a filename is a proper shard name."""
    return file_name.startswith("shard") and len(file_name) > 5 and file_name[5:].isdigit()


def is_index(file_name: str) -> bool:
    """Determines if a filename is a proper index name."""
    return file_name == "index"


def is_info(file_name: str) -> bool:
    """Determines if a filename is a proper info name."""
    return file_name == "info"


def create_index_path(directory: str) -> str:
    """Creates an index path for a given directory."""
    return path.join(directory, "index")


def create_info_path(directory: str) -> str:
    """Creates an info path for a given directory."""
    return path.join(directory, "info")


def create_shard_path(directory: str, shard_number: int) -> str:
    """Creates a shard path for a given directory and shard number."""
    return f"""{path.join(directory, "shard")}{shard_number}"""


def get_shard_number(file_name: str) -> int:
    """Retrieves the shard number from a shard filename."""
    return int(file_name[5:])


def get_shard_file_paths(directory: str) -> Dict[int, str]:
    """Gets all of the shard files in a table directory."""
    return dict((get_shard_number(file), path.join(directory, file)) for file in
                [file for file in listdir(directory) if is_shard(file)])


def valid_table_contents(dir_path: str) -> bool:
    """Validates that a table has all of the necessary information to load data."""
    files = [file for file in listdir(dir_path)]
    index_files = [file for file in files if is_index(file)]
    shard_files = [file for file in files if is_shard(file)]
    info_files = [file for file in files if is_info(file)]
    if len(index_files) < 1 or len(shard_files) < 1 or len(info_files) < 1:
        return False
    return True
