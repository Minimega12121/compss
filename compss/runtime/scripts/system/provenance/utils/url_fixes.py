import typing

from urllib.parse import urlsplit


def fix_dir_url(in_url: str) -> str:
    """
    Fix dir:// URL returned by the runtime, change it to file:// and ensure it ends with '/'

    :param in_url: URL that may need to be fixed

    :returns: A file:// URL
    """

    runtime_url = urlsplit(in_url)
    if (
        runtime_url.scheme == "dir"
    ):  # Fix dir:// to file:// and ensure it ends with a slash
        new_url = "file://" + runtime_url.netloc + runtime_url.path
        if new_url[-1] != "/":
            new_url += "/"  # Add end slash if needed
        return new_url
    # else
    return in_url  # No changes required