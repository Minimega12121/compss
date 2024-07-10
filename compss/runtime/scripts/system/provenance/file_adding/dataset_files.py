import typing
import os
import datetime as dt

from urllib.parse import urlsplit
from pathlib import Path
from datetime import timezone

# from ..utils.url_fixes import fix_dir_url
# TODO: FIX SIBLING PACKAGE REFERENCE

from rocrate.rocrate import ROCrate
from rocrate.utils import iso_now


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


def add_dataset_file_to_crate(
    compss_crate: ROCrate, in_url: str, persist: bool, common_paths: list
) -> str:
    """
    Add the file (or a reference to it) belonging to the dataset of the application (both input or output)
    When adding local files that we don't want to be physically in the Crate, they must be added with a file:// URI
    CAUTION: If the file has been already added (e.g. for INOUT files) add_file won't succeed in adding a second entity
    with the same name

    :param compss_crate: The COMPSs RO-Crate being generated
    :param in_url: File added as input or output
    :param persist: True to attach the file to the crate, False otherwise
    :param common_paths: List of identified common paths among all dataset files, all finish with '/'

    :returns: The original url if persist is false, the crate_path if persist is true
    """

    # method_time = time.time()

    url_parts = urlsplit(in_url)
    # If in_url ends up with '/', os.path.basename will be empty, thus we need Pathlib
    url_path = Path(url_parts.path)
    final_item_name = url_path.name

    if url_parts.scheme in ["dir", "file"]:
        # Dealing with a local file
        file_properties = {
            "name": final_item_name,
            "sdDatePublished": iso_now(),
            "dateModified": dt.datetime.fromtimestamp(
                os.path.getmtime(url_parts.path), timezone.utc
            )
            .replace(microsecond=0)
            .isoformat(),  # Schema.org
        }  # Register when the Data Entity was last accessible
    else:
        # Remote file
        file_properties = {"name": final_item_name}

    if url_parts.scheme == "file":  # Dealing with a local file
        file_properties["contentSize"] = os.path.getsize(url_parts.path)
        crate_path = ""
        # add_file_time = time.time()
        if persist:  # Remove scheme so it is added as a regular file
            for i, item in enumerate(common_paths):  # All files must have a match
                if url_parts.path.startswith(item):
                    cwd_endslash = (
                        os.getcwd() + "/"
                    )  # os.getcwd does not add the final slash
                    if cwd_endslash.startswith("/gpfs/home/"):
                        # BSC hack, /gpfs/home/ and /home/ are equivalent
                        cwd_final = cwd_endslash[5:]
                    else:
                        cwd_final = cwd_endslash
                    if cwd_final == item:
                        # Check if it is the working directory. When this script runs, user application has finished,
                        # so we can ensure cwd is the original folder where the application was started
                        # Workingdir dataset folder, add it to the root
                        crate_path = "dataset/" + url_parts.path[len(item) :]
                        # Slice out the common part of the path
                    else:  # Now includes len(common_paths) == 1
                        cp_path = Path(
                            item
                        )  # Looking for the name of the previous folder
                        crate_path = (
                            "dataset/"
                            # + "folder_"
                            # + str(i)
                            + cp_path.parts[
                                -1
                            ]  # Base name of the identified common path. Now it does not avoid collisions if the user defines the same folder name in two different locations
                            + "/"  # Common part now always ends with '/'
                            + url_parts.path[len(item) :]
                        )  # Slice out the common part of the path
                    break
            # print(f"PROVENANCE DEBUG | Adding {url_parts.path} as {crate_path}")
            compss_crate.add_file(
                source=url_parts.path, dest_path=crate_path, properties=file_properties
            )
            return crate_path
        # else:
        compss_crate.add_file(
            in_url,
            fetch_remote=False,
            validate_url=False,  # True fails at MN4 when file URI points to a node hostname (only localhost works)
            properties=file_properties,
        )
        return in_url
        # add_file_time = time.time() - add_file_time

    if url_parts.scheme == "dir":  # DIRECTORY parameter
        # if persist:
        #     # Add whole dataset, and return. Clean path name first
        #     crate_path = "dataset/" + final_item_name
        #     print(f"PROVENANCE DEBUG | Adding DATASET {url_parts.path} as {crate_path}")
        #     compss_crate.add_tree(source=url_parts.path, dest_path=crate_path, properties=file_properties)
        #     # fetch_remote and validate_url false by default. add_dataset also ensures the URL ends with '/'
        #     return crate_path

        # For directories, describe all files inside the directory
        has_part_list = []
        for root, dirs, files in os.walk(
            url_parts.path, topdown=True, followlinks=True
        ):  # Ignore references to sub-directories (they are not a specific in or out of the workflow),
            # but not their files
            if "__pycache__" in root:
                continue  # We skip __pycache__ subdirectories
            dirs.sort()
            files.sort()
            for f_name in files:
                if f_name.startswith("*"):
                    # Avoid dealing with symlinks with wildcards
                    continue
                listed_file = os.path.join(root, f_name)
                # print(f"PROVENANCE DEBUG: listed_file is {listed_file}")
                dir_f_properties = {
                    "name": f_name,
                    "sdDatePublished": iso_now(),  # Register when the Data Entity was last accessible
                    "dateModified": dt.datetime.fromtimestamp(
                        os.path.getmtime(listed_file), timezone.utc
                    )
                    .replace(microsecond=0)
                    .isoformat(),
                    # Schema.org
                    "contentSize": os.path.getsize(listed_file),
                }
                if persist:
                    # url_parts.path includes a final '/'
                    filtered_url = listed_file[
                        len(url_parts.path) :
                    ]  # Does not include an initial '/'
                    dir_f_url = "dataset/" + final_item_name + "/" + filtered_url
                    # print(f"PROVENANCE DEBUG | Adding DATASET FILE {listed_file} as {dir_f_url}")
                    compss_crate.add_file(
                        source=listed_file,
                        dest_path=dir_f_url,
                        fetch_remote=False,
                        validate_url=False,
                        # True fails at MN4 when file URI points to a node hostname (only localhost works)
                        properties=dir_f_properties,
                    )
                else:
                    dir_f_url = "file://" + url_parts.netloc + listed_file
                    compss_crate.add_file(
                        dir_f_url,
                        fetch_remote=False,
                        validate_url=False,
                        # True fails at MN4 when file URI points to a node hostname (only localhost works)
                        properties=dir_f_properties,
                    )
                has_part_list.append({"@id": dir_f_url})

            for dir_name in dirs:
                # Check if it's an empty directory, needs to be added by hand
                full_dir_name = os.path.join(root, dir_name)
                if not os.listdir(full_dir_name):
                    # print(f"PROVENANCE DEBUG | Adding an empty directory in data persistence. root ({root}), full_dir_name ({full_dir_name})")
                    dir_properties = {
                        "sdDatePublished": iso_now(),
                        "dateModified": dt.datetime.fromtimestamp(
                            os.path.getmtime(full_dir_name), timezone.utc
                        )
                        .replace(microsecond=0)
                        .isoformat(),  # Schema.org
                    }  # Register when the Data Entity was last accessible
                    if persist:
                        # Workaround to add empty directories in a git repository
                        git_keep = Path(full_dir_name + "/" + ".gitkeep")
                        Path.touch(git_keep)
                        dir_properties["name"] = ".gitkeep"
                        path_final_part = full_dir_name[len(url_parts.path) :]
                        dir_f_url = (
                            "dataset/"
                            + final_item_name
                            + "/"
                            + path_final_part
                            + "/"
                            + ".gitkeep"
                        )
                        # compss_crate.add_dataset(
                        #     source=full_dir_name,
                        #     dest_path=dir_f_url,
                        #     properties=dir_properties,
                        # )
                        # print(f"ADDING DATASET FILE {git_keep} as {dir_f_url}")
                        compss_crate.add_file(
                            source=git_keep,
                            dest_path=dir_f_url,
                            fetch_remote=False,
                            validate_url=False,
                            # True fails at MN4 when file URI points to a node hostname (only localhost works)
                            properties=dir_properties,
                        )
                    else:
                        dir_properties["name"] = dir_name
                        dir_f_url = "file://" + url_parts.netloc + full_dir_name + "/"
                        # Directories must finish with slash
                        compss_crate.add_dataset(
                            source=dir_f_url, properties=dir_properties
                        )
                        has_part_list.append({"@id": dir_f_url})

        # After checking all directory structure, represent correctly the dataset
        if not os.listdir(url_parts.path):
            # The root directory itself is empty
            # print(f"PROVENANCE DEBUG | Adding an empty directory. url_parts.path ({url_parts.path})")
            if persist:
                # Workaround to add empty directories in a git repository
                git_keep = Path(url_parts.path + "/" + ".gitkeep")
                Path.touch(git_keep)
                dir_properties = {
                    "name": ".gitkeep",
                    "sdDatePublished": iso_now(),
                    "dateModified": dt.datetime.fromtimestamp(
                        os.path.getmtime(url_parts.path), timezone.utc
                    )
                    .replace(microsecond=0)
                    .isoformat(),  # Schema.org
                }  # Register when the Data Entity was last accessible
                path_in_crate = (
                    "dataset/" + final_item_name + "/" + ".gitkeep"
                )  # Remove resolved_source from full_dir_name, adding basename
                # compss_crate.add_dataset(
                #     source=full_dir_name,
                #     dest_path=path_in_crate,
                #     properties=dir_properties,
                # )
                # print(f"ADDING FILE {git_keep} as {path_in_crate}")
                compss_crate.add_file(
                    source=git_keep,
                    dest_path=path_in_crate,
                    fetch_remote=False,
                    validate_url=False,
                    # True fails at MN4 when file URI points to a node hostname (only localhost works)
                    properties=dir_properties,
                )
                has_part_list.append({"@id": path_in_crate})
                path_in_crate = "dataset/" + final_item_name + "/"
                # fetch_remote and validate_url false by default. add_dataset also ensures the URL ends with '/'
                dir_properties["name"] = final_item_name
                dir_properties["hasPart"] = has_part_list
                # print(f"ADDING DATASET FOR THE EMPTY DIRECTORY {final_item_name} as {path_in_crate}, with hasPart {has_part_list}")
                compss_crate.add_dataset(
                    source=url_parts.path,
                    dest_path=path_in_crate,
                    properties=dir_properties,
                )
                return path_in_crate
            else:
                # Directories must finish with slash
                compss_crate.add_dataset(
                    source=fix_dir_url(in_url), properties=file_properties
                )
        else:
            # Directory had content
            file_properties["hasPart"] = has_part_list
            if persist:
                dataset_path = url_parts.path
                path_in_crate = "dataset/" + final_item_name + "/"
                # print(f"PROVENANCE DEBUG | Adding DATASET {dataset_path} as {path_in_crate}")
                compss_crate.add_dataset(
                    source=dataset_path,
                    dest_path=path_in_crate,
                    properties=file_properties,
                )  # fetch_remote and validate_url false by default. add_dataset also ensures the URL ends with '/'
                return path_in_crate
            # else:
            # fetch_remote and validate_url false by default. add_dataset also ensures the URL ends with '/'
            compss_crate.add_dataset(fix_dir_url(in_url), properties=file_properties)

    if url_parts.scheme.startswith("http"):
        # Remote file, currently not supported in COMPSs. validate_url=True already adds contentSize and encodingFormat
        # from the remote file
        val_url = True
        if os.getenv("BSC_MACHINE"):
            # Cluster without outside connectivity (e.g. at BSC)
            val_url = False
        compss_crate.add_file(
            source=in_url,
            validate_url=val_url,
            fetch_remote=False,
            properties=file_properties,
        )

    # print(f"Method vs add_file TIME: {time.time() - method_time} vs {add_file_time}")

    return fix_dir_url(in_url)
