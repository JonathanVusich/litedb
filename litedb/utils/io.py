import os
import shutil
import stat


def _remove_readonly(func, path, _):
    """Clear the readonly bit and reattempt the removal."""
    os.chmod(path, stat.S_IWRITE)
    func(path)


def rmdir(directory):
    """Removes the given directory entirely."""
    shutil.rmtree(directory, onerror=_remove_readonly)


def empty_directory(directory) -> None:
    """Removes the contents of a directory."""
    with os.scandir(directory) as dir_contents:
        for entry in dir_contents:
            if entry.is_file():
                os.unlink(entry)
            if entry.is_dir():
                rmdir(entry)
