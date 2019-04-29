from os import scandir, listdir, DirEntry, path
from typing import List, Generator, Tuple, Dict

from ..errors import DatabaseNotFound


def load_tables(directory: str) -> str:
    tables = 0
    with scandir(path=directory) as curdir:
        for entry in curdir:
            if is_table_folder(entry):
                tables += 1
                yield entry
    if tables == 0:
        raise DatabaseNotFound


def is_table_folder(directory: DirEntry) -> bool:
    return directory.is_dir(follow_symlinks=False) and is_table(directory.name) and valid_table_contents(directory.path)


def is_table(file_name: str) -> bool:
    return file_name.startswith("table") and len(file_name) > 5 and file_name[5:].isdigit()


def is_shard_file(file: DirEntry) -> bool:
    return is_shard(file.name) and file.is_file(follow_symlinks=False)


def is_index_file(file: DirEntry) -> bool:
    return is_index(file.name) and file.is_file(follow_symlinks=False)


def is_info_file(file: DirEntry) -> bool:
    return is_info(file.name) and file.is_file(follow_symlinks=False)


def is_shard(file_name: str) -> bool:
    return file_name.startswith("shard") and len(file_name) > 5 and file_name[5:].isdigit()


def is_index(file_name: str) -> bool:
    return file_name == "index"


def is_info(file_name: str) -> bool:
    return file_name == "info"


def create_index_path(directory: str) -> str:
    return path.join(directory, "index")


def create_info_path(directory: str) -> str:
    return path.join(directory, "info")


def create_shard_path(directory: str, shard_number: int) -> str:
    return f"""{path.join(directory, "shard")}{shard_number}"""


def get_shard_number(file_name: str) -> int:
    return int(file_name[5:])


def get_shard_file_paths(directory: str) -> Dict[int, str]:
    return dict((get_shard_number(file), path.join(directory, file)) for file in [file for file in listdir(directory) if is_shard(file)])


def valid_table_contents(dir_path: str) -> bool:
    files = [file for file in listdir(dir_path)]
    index_files = [file for file in files if is_index(file)]
    shard_files = [file for file in files if is_shard(file)]
    info_files = [file for file in files if is_info(file)]
    if len(index_files) < 1 or len(shard_files) < 1 or len(info_files) < 1:
        return False
    return True
