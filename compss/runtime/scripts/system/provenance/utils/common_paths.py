#!/usr/bin/python
#
#  Copyright 2002-2023 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import typing
import os

from urllib.parse import urlsplit
from pathlib import Path


def get_common_paths(url_list: list) -> list:
    """
    Find the common paths in the list of files passed.

    :param url_list: Sorted list of file URLs as generated by COMPSs runtime

    :returns: List of identified common paths among the URLs
    """

    list_common_paths = []  # Create common_paths list, with counter of occurrences
    if not url_list:  # Empty list
        return list_common_paths

    # The list comes ordered, so all dir:// references will come first, file:// next, and https:// last
    # We don't need to skip the directories, we need to add them, since they are common paths already
    i_files = -1  # Index where files start
    i_url = -1  # Index where urls start
    file_found = False
    for i, item in enumerate(url_list):
        url_parts = urlsplit(item)
        if url_parts.scheme == "https":
            i_url = i
            break
        elif url_parts.scheme == "dir":
            if url_parts.path not in list_common_paths:
                list_common_paths.append(url_parts.path)
                if __debug__:
                    print(
                        f"PROVENANCE DEBUG | ADDING DIRECTORY AS COMMON_PATH {url_parts.path}"
                    )
        else:
            # file://
            if not file_found:
                i_files = i
                file_found = True

    if not file_found:
        # All are directories
        if __debug__:
            print(
                f"PROVENANCE DEBUG | Resulting list of common paths with only directories is: {list_common_paths}"
            )
        return list_common_paths

    # Add first found file
    url_parts = urlsplit(url_list[i_files])
    # Need to remove schema and hostname from reference, and filename
    common_path = str(Path(url_parts.path).parents[0])

    url_files_list = url_list[
        i_files + 1 : i_url
    ]  # Slice out directories, urls, and the first file
    if __debug__:
        print(
            f"PROVENANCE DEBUG | List of files only (removing first): {url_files_list}"
        )
    for item in url_files_list:
        # url_list is a sorted list, important for this algorithm to work
        # if item and common_path have a common path, store that common path in common_path and continue, until the
        # shortest common path different than 0 has been identified
        # https://docs.python.org/3/library/os.path.html  # os.path.commonpath

        url_parts = urlsplit(item)
        # Remove schema and hostname
        # url_parts.path does not end with '/'
        tmp = os.path.commonpath([url_parts.path, common_path])
        if tmp != "/":  # String not empty, they have a common path
            if __debug__:
                print(f"PROVENANCE DEBUG | Searching. Previous common path is: {common_path}. tmp: {tmp}")
            common_path = tmp
        else:  # if they don't, we are in a new path, so, store the previous in list_common_paths, and assign the new to common_path
            if __debug__:
                print(f"PROVENANCE DEBUG | New root to search common_path: {url_parts.path}")
            if common_path not in list_common_paths:
                list_common_paths.append(common_path + "/")
            # Need to remove filename from url_parts.path
            common_path = str(Path(url_parts.path).parents[0])

    # Add last element's path
    if common_path not in list_common_paths:
        list_common_paths.append(common_path)

    # All paths internally need to finish with a '/'
    for i, item in enumerate(list_common_paths):
        if item[-1] != "/":
            list_common_paths[i] += "/"

    if __debug__:
        print(
            f"PROVENANCE DEBUG | Resulting list of common paths is: {list_common_paths}"
        )

    return list_common_paths
