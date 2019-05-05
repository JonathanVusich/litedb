import os, stat
import shutil


def _remove_readonly(func, path, _):
    """Clear the readonly bit and reattempt the removal."""
    os.chmod(path, stat.S_IWRITE)
    func(path)


def rmdir(directory):
    """Removes the given directory entirely."""
    shutil.rmtree(directory, onerror=_remove_readonly)
