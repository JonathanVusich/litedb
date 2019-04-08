from os import scandir


def dir_empty(directory: str) -> bool:
    # Check that the path is an empty directory
    with scandir(path=directory) as curdir:
        for entry in curdir:
            if entry.is_file() or entry.is_dir():
                return False
    return True
