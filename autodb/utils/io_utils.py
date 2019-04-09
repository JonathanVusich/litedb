from os import scandir, listdir, DirEntry
from typing import List, Generator, Tuple

from ..errors import DatabaseNotFound


def dir_empty(directory: str) -> bool:
    # Check that the path is an empty directory
    with scandir(path=directory) as curdir:
        for entry in curdir:
            if entry.is_file(follow_symlinks=False) or entry.is_dir(follow_symlinks=False):
                return False
    return True


def load_database(directory: str) -> Generator[Tuple[str, str, List[str]], None, None]:
    tables = 0
    with scandir(path=directory) as curdir:
        for entry in curdir:
            if is_table_folder(entry):
                tables += 1
                yield retrieve_table_from_directory(entry)
    if tables == 0:
        raise DatabaseNotFound


def retrieve_table_from_directory(directory: DirEntry) -> Tuple[str, str, List[str]]:
    table_path = directory.path
    index = None
    shards = []
    with scandir(path=table_path) as curdir:
        for entry in curdir:
            if is_index_file(entry):
                index = entry.path
            elif is_shard_file(entry):
                shards.append(entry.path)
    return table_path, index, shards


def is_table_folder(directory: DirEntry) -> bool:
    return directory.is_dir(follow_symlinks=False) and is_table(directory.name) and valid_table_contents(directory.path)


def is_table(file_name: str) -> bool:
    return file_name.startswith("table") and len(file_name) > 5 and file_name[5:].isdigit()


def is_shard_file(file: DirEntry) -> bool:
    return is_shard(file.name) and file.is_file(follow_symlinks=False)


def is_index_file(file: DirEntry) -> bool:
    return is_index(file.name) and file.is_file(follow_symlinks=False)


def is_shard(file_name: str) -> bool:
    return file_name.startswith("shard") and len(file_name) > 5 and file_name[5:].isdigit()


def is_index(file_name: str) -> bool:
    return file_name == "index"


def valid_table_contents(dir_path: str) -> bool:
    files = [file for file in listdir(dir_path)]
    index_files = [file for file in files if is_index(file)]
    shard_files = [file for file in files if is_shard(file)]
    if len(index_files) < 1 or len(shard_files) < 1:
        return False
    return True
