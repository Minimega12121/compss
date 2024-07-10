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

"""
    The generate_COMPSs_RO-Crate.py module generates the resulting RO-Crate metadata from a COMPSs application run
    following the Workflow Run Crate profile specification. Takes as parameters the ro-crate-info.yaml, and the
    dataprovenance.log generated from the run.
"""

from pathlib import Path
from urllib.parse import urlsplit
from datetime import datetime, timezone
import os
import uuid
import typing
import datetime as dt
import socket
import subprocess
import yaml
import time
import sys

from rocrate.rocrate import ROCrate
from rocrate.model.person import Person
from rocrate.model.contextentity import ContextEntity

# from rocrate.model.entity import Entity
# from rocrate.model.file import File
from rocrate.utils import iso_now

from processing.entities import root_entity, add_person_definition, get_main_entities
from processing.files import process_accessed_files
from file_adding.source_files import add_application_source_files


PROFILES_BASE = "https://w3id.org/ro/wfrun"
PROFILES_VERSION = "0.5"
WROC_PROFILE_VERSION = "1.0"


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


def wrroc_create_action(
    compss_crate: ROCrate,
    main_entity: str,
    author_list: list,
    ins: list,
    outs: list,
    yaml_content: dict,
) -> str:
    """
    Add a CreateAction term to the ROCrate to make it compliant with WRROC.  RO-Crate WorkflowRun Level 2 profile,
    aka. Workflow Run Crate.

    :param compss_crate: The COMPSs RO-Crate being generated
    :param main_entity: The name of the source file that contains the COMPSs application main() method
    :param author_list: List of authors as described in the YAML
    :param ins: List of input files of the workflow
    :param outs: List of output files of the workflow
    :param yaml_content: Content of the YAML file specified by the user

    :returns: UUID generated for this run
    """

    # Compliance with RO-Crate WorkflowRun Level 2 profile, aka. Workflow Run Crate
    # marenostrum4, nord3, ... BSC_MACHINE would also work
    host_name = os.getenv("SLURM_CLUSTER_NAME")
    if host_name is None:
        host_name = os.getenv("BSC_MACHINE")
        if host_name is None:
            host_name = socket.gethostname()
    job_id = os.getenv("SLURM_JOB_ID")

    main_entity_pathobj = Path(main_entity)

    run_uuid = str(uuid.uuid4())

    if job_id is None:
        name_property = (
            "COMPSs " + main_entity_pathobj.name + " execution at " + host_name
        )
        userportal_url = None
        create_action_id = "#COMPSs_Workflow_Run_Crate_" + host_name + "_" + run_uuid
    else:
        name_property = (
            "COMPSs "
            + main_entity_pathobj.name
            + " execution at "
            + host_name
            + " with JOB_ID "
            + job_id
        )
        userportal_url = "https://userportal.bsc.es/"  # job_id cannot be added, does not match the one in userportal
        create_action_id = (
            "#COMPSs_Workflow_Run_Crate_" + host_name + "_SLURM_JOB_ID_" + job_id
        )
    compss_crate.root_dataset["mentions"] = {"@id": create_action_id}

    # OSTYPE, HOSTTYPE, HOSTNAME defined by bash and not inherited. Changed to "uname -a"
    uname = subprocess.run(["uname", "-a"], stdout=subprocess.PIPE, check=True)
    uname_out = uname.stdout.decode("utf-8")[:-1]  # Remove final '\n'

    # SLURM interesting variables: SLURM_JOB_NAME, SLURM_JOB_QOS, SLURM_JOB_USER, SLURM_SUBMIT_DIR, SLURM_NNODES or
    # SLURM_JOB_NUM_NODES, SLURM_JOB_CPUS_PER_NODE, SLURM_MEM_PER_CPU, SLURM_JOB_NODELIST or SLURM_NODELIST.

    environment_property = []
    for name, value in os.environ.items():
        if (
            name.startswith(("SLURM_JOB", "SLURM_MEM", "SLURM_SUBMIT", "COMPSS"))
            and name != "SLURM_JOBID"
        ):
            # Changed to 'environment' term in WRROC v0.4
            env_var = {}
            env_var["@type"] = "PropertyValue"
            env_var["name"] = name
            env_var["value"] = value
            compss_crate.add(
                ContextEntity(
                    compss_crate,
                    "#" + name.lower(),
                    properties=env_var,
                )
            )
            environment_property.append({"@id": "#" + name.lower()})

    description_property = uname_out

    resolved_main_entity = main_entity
    for entity in compss_crate.get_entities():
        if "ComputationalWorkflow" in entity.type:
            resolved_main_entity = entity.id

    # Register user submitting the workflow
    if "Agent" in yaml_content and add_person_definition(
        compss_crate, "Agent", yaml_content["Agent"], INFO_YAML
    ):
        agent = {"@id": yaml_content["Agent"]["orcid"]}
    else:  # Choose first author, to avoid leaving it empty. May be true most of the times
        if author_list:
            agent = author_list[0]
            print(
                f"PROVENANCE | WARNING: 'Agent' not specified in {INFO_YAML}. First author selected by default."
            )
        else:
            agent = None
            print(
                f"PROVENANCE | WARNING: No 'Authors' or 'Agent' specified in {INFO_YAML}"
            )

    create_action_properties = {
        "@type": "CreateAction",
        "instrument": {"@id": resolved_main_entity},  # Resolved path of the main file
        "actionStatus": {"@id": "http://schema.org/CompletedActionStatus"},
        "endTime": iso_now(),  # Get current time
        "name": name_property,
        "description": description_property,
    }
    if len(environment_property) > 0:
        create_action_properties["environment"] = environment_property

    if job_id:
        sacct_command = ["sacct", "-j", str(job_id), "--format=Start", "--noheader"]
        head_command = ["head", "-n", "1"]
        sacct_process = subprocess.Popen(sacct_command, stdout=subprocess.PIPE)
        head_process = subprocess.Popen(
            head_command, stdin=sacct_process.stdout, stdout=subprocess.PIPE
        )
        output, _ = head_process.communicate()
        start_time_str = output.decode("utf-8").strip()
        # Convert start time to datetime object
        start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%S")
        create_action_properties["startTime"] = start_time.astimezone(
            timezone.utc
        ).isoformat()
    else:
        # Take startTime from dataprovenance.log when no queuing system is involved
        # The string generated by the runtime is already in UTC
        with open(DP_LOG, "r", encoding="UTF-8") as dp_file:
            for i, line in enumerate(dp_file):
                if i == 3:
                    start_time = datetime.strptime(
                        line.strip(), "%Y-%m-%dT%H:%M:%S.%f%z"
                    )
                    create_action_properties["startTime"] = start_time.replace(
                        microsecond=0
                    ).isoformat()
                    break

    if agent:
        create_action_properties["agent"] = agent

    create_action = compss_crate.add(
        ContextEntity(compss_crate, create_action_id, create_action_properties)
    )  # id can be something fancy for MN4, otherwise, whatever
    create_action.properties()

    # "subjectOf": {"@id": userportal_url}
    if userportal_url is not None:
        create_action.append_to("subjectOf", userportal_url)

    # "object": [{"@id":}],  # List of inputs
    # "result": [{"@id":}]  # List of outputs
    # Right now neither the COMPSs runtime nor this script check if a file URI is inside a dir URI. This means
    # duplicated entries can be found in the metadata (i.e. a file that is part of a directory, can be added
    # independently). However, this does not add duplicated files if data_persistence is True
    # Hint for controlling duplicates: both 'ins' and 'outs' dir URIs come first on each list
    for item in ins:
        create_action.append_to("object", {"@id": fix_dir_url(item)})
    for item in outs:
        create_action.append_to("result", {"@id": fix_dir_url(item)})
    create_action.append_to("result", {"@id": "./"})  # The generated RO-Crate

    # Add out and err logs in SLURM executions
    if job_id:
        suffix = [".out", ".err"]
        msg = ["output", "error"]
        for f_suffix, f_msg in zip(suffix, msg):
            file_properties = {}
            file_properties["name"] = "compss-" + job_id + f_suffix
            file_properties["contentSize"] = os.path.getsize(file_properties["name"])
            file_properties["description"] = (
                "COMPSs console standard " + f_msg + " log file"
            )
            file_properties["encodingFormat"] = "text/plain"
            file_properties["about"] = create_action_id
            compss_crate.add_file(file_properties["name"], properties=file_properties)

    return run_uuid


def get_common_paths(url_list: list) -> list:
    """
    Find the common paths in the list of files passed.

    :param url_list: Sorted list of file URLs as generated by COMPSs runtime

    :returns: List of identified common paths among the URLs
    """

    # print(f"PROVENANCE DEBUG | Input to get_common_paths INS and OUTS: {url_list}")
    list_common_paths = []  # Create common_paths list, with counter of occurrences
    if not url_list:  # Empty list
        return list_common_paths

    # The list comes ordered, so all dir:// references will come first
    # We don't need to skip them, we need to add them, since they are common paths already
    i = 0
    file_found = False
    for item in url_list:
        url_parts = urlsplit(item)
        if url_parts.scheme == "dir":
            if url_parts.path not in list_common_paths:
                list_common_paths.append(url_parts.path)
            i += 1
            # print(f"PROVENANCE DEBUG | ADDING DIRECTORY AS COMMON_PATH {url_parts.path}")
            continue
        else:
            file_found = True
            break

    if not file_found:
        # All are directories
        # print(f"PROVENANCE DEBUG | Resulting list of common paths with only directories is: {list_common_paths}")
        return list_common_paths

    # Add first found file
    url_parts = urlsplit(url_list[i])
    # Need to remove schema and hostname from reference, and filename
    common_path = str(Path(url_parts.path).parents[0])
    i += 1

    url_files_list = url_list[i:]  # Slice out directories and the first file
    for item in url_files_list:
        # url_list is a sorted list, important for this algorithm to work
        # if item and common_path have a common path, store that common path in common_path and continue, until the
        # shortest common path different than 0 has been identified
        # https://docs.python.org/3/library/os.path.html  # os.path.commonpath

        url_parts = urlsplit(item)
        # Remove schema and hostname
        tmp = os.path.commonpath(
            [url_parts.path, common_path]
        )  # url_parts.path does not end with '/'
        if tmp != "/":  # String not empty, they have a common path
            # print(f"PROVENANCE DEBUG | Searching. Previous common path is: {common_path}. tmp: {tmp}")
            common_path = tmp
        else:  # if they don't, we are in a new path, so, store the previous in list_common_paths, and assign the new to common_path
            # print(f"PROVENANCE DEBUG | New root to search common_path: {url_parts.path}")
            if common_path not in list_common_paths:
                list_common_paths.append(common_path)
            common_path = str(
                Path(url_parts.path).parents[0]
            )  # Need to remove filename from url_parts.path

    # Add last element's path
    if common_path not in list_common_paths:
        list_common_paths.append(common_path)

    # All paths internally need to finish with a '/'
    for item in list_common_paths:
        if item[-1] != "/":
            list_common_paths.append(item + "/")
            list_common_paths.remove(item)

    # print(f"PROVENANCE DEBUG | Resulting list of common paths is: {list_common_paths}")

    return list_common_paths


def add_manual_datasets(yaml_term: str, compss_wf_info: dict, data_list: list) -> list:
    """
    Adds to a list of dataset entities (files or directories) the ones specified by the user. At the end, removes any
    file:// references that belong to other dir:// references

    :param yaml_term: Term specified in the YAML file (i.e. 'inputs' or 'outputs')
    :param compss_wf_info: YAML dict to extract info form the application, as specified by the user
    :param data_list: Sorted list of file and dir URLs as generated by COMPSs runtime

    :returns: Updated List of identified common paths among the URLs
    """

    # Add entities defined by the user
    # Input files or directories added by hand from the user
    data_entities_list = []
    if isinstance(compss_wf_info[yaml_term], list):
        data_entities_list.extend(compss_wf_info[yaml_term])
    else:
        data_entities_list.append(compss_wf_info[yaml_term])
    for item in data_entities_list:
        # Check if remote file with URI scheme: http or https
        url_parts = urlsplit(item)
        if url_parts.scheme.startswith("http"):
            data_list.append(item)
            continue

        path_data_entity = Path(item).expanduser()

        if not path_data_entity.exists():
            print(
                f"PROVENANCE | WARNING: A file or directory defined as '{yaml_term}' in {INFO_YAML} does not exist "
                f"({item})"
            )
            continue
        first_resolved_data_entity = str(path_data_entity.resolve())

        # BSC hack: /gpfs/home/ and /home/ are the same path
        if first_resolved_data_entity.startswith("/gpfs/home/"):
            # BSC hack, /gpfs/home/ and /home/ are equivalent
            resolved_data_entity = first_resolved_data_entity[5:]
        else:
            resolved_data_entity = first_resolved_data_entity

        if os.path.isfile(resolved_data_entity):
            new_data_entity = "file://" + socket.gethostname() + resolved_data_entity
        elif os.path.isdir(resolved_data_entity):
            new_data_entity = (
                "dir://" + socket.gethostname() + resolved_data_entity + "/"
            )
        else:
            print(
                f"FATAL ERROR: a reference is neither a file, nor a directory ({resolved_data_entity})"
            )
            raise FileNotFoundError
        if new_data_entity not in data_list:
            # Checking if a file is in a dir would be costly
            data_list.append(new_data_entity)
        else:
            print(
                f"PROVENANCE | WARNING: A file or directory defined as '{yaml_term}' in {INFO_YAML} was already part of the dataset "
                f"({item})"
            )
    data_list.sort()  # Sort again, needed for next methods applied to the list

    # POSSIBLE TODO: keep dir and files in separated lists to avoid traversing them too many times, improving efficiency

    # Now erase any file:// that is inside dir://
    i = 0
    directories_list = []
    file_found = False
    for item in data_list:
        url_parts = urlsplit(item)
        if url_parts.scheme == "dir":
            if url_parts.path not in directories_list and not (
                any(
                    url_parts.path.startswith(dir_item) for dir_item in directories_list
                )
            ):
                directories_list.append(url_parts.path)
            i += 1
            continue
        else:
            file_found = True
            break
    if file_found:  # Not all are directories
        #  TODO Can this two loops be merged????
        data_list_copy = (
            data_list.copy()
        )  # So we can remove the element we are iterating upon

        for item in data_list_copy:
            # Check both dir:// and file:// references
            url_parts = urlsplit(item)
            if any(
                (url_parts.path != dir_path and url_parts.path.startswith(dir_path))
                for dir_path in directories_list
            ):
                # If the url dir:// does not finish with a slash, can add errors (e.g. /inputs vs /inputs.zip)
                print(
                    f"PROVENANCE | WARNING: Item {url_parts.path} removed as {yaml_term}, since it already belongs to a dataset"
                )
                data_list.remove(item)

            # if any((url_parts.path != dir_path and url_parts.path.startswith(dir_path)) for dir_path in directories_list):
            #     # if the url dir:// does not finish with a slash, can add errors (e.g. /inputs vs /inputs.zip)
            #     print(
            #         f"PROVENANCE | WARNING: Item {item} removed as {yaml_term}, since it already belongs to a dataset"
            #     )
            #     data_list.remove(item)

    print(
        f"PROVENANCE | Manually added data assets as '{yaml_term}' ({len(data_entities_list)})"
    )

    return data_list


def fix_in_files_at_out_dirs(
    inputs_list: list, outputs_list: list
) -> typing.Tuple[list, list]:
    """
    Remove any file inputs that the user may have declared as directory outputs

    :param inputs_list: list of all input directories and files as URLs
    :param compss_wf_info: list of all output directories and files as URLs


    :returns: Updated inputs and outputs lists
    """

    # Now erase any file:// input that is included as dir:// output
    directories_list = []
    file_found = False
    for item in outputs_list:
        url_parts = urlsplit(item)
        if url_parts.scheme == "dir":
            if url_parts.path not in directories_list:
                directories_list.append(url_parts.path)
        else:
            file_found = True
            break

    i = 0
    file_found = False
    for item in inputs_list:
        url_parts = urlsplit(item)
        if url_parts.scheme == "dir":
            i += 1
        else:
            file_found = True
            break

    if not file_found:
        return inputs_list, outputs_list

    url_files_list = inputs_list[i:]  # Slice out directories
    for item in url_files_list:
        url_parts = urlsplit(item)
        if any(url_parts.path.startswith(dir_path) for dir_path in directories_list):
            print(
                f"PROVENANCE | WARNING: Metadata of an input file has been removed since it is included at an output directory: {url_parts.path}"
            )
            inputs_list.remove(item)

    # print(f"PROVENANCE DEBUG | RESULT FROM fix_in_files_at_out_dirs:\n {inputs_list}")

    return inputs_list, outputs_list


def main():
    """
    Generate an RO-Crate from a COMPSs execution dataprovenance.log file.

    :param None

    :returns: None
    """

    exec_time = time.time()

    yaml_template = (
        "COMPSs Workflow Information:\n"
        "  name: Name of your COMPSs application\n"
        "  description: Detailed description of your COMPSs application\n"
        "  license: Apache-2.0\n"
        "    # URL preferred, but these strings are accepted: https://about.workflowhub.eu/Workflow-RO-Crate/#supported-licenses\n"
        "  sources: [/absolute_path_to/dir_1/, relative_path_to/dir_2/, main_file.py, relative_path/aux_file_1.py, /abs_path/aux_file_2.py]\n"
        "    # List of application source files and directories. Relative or absolute paths can be used.\n"
        "  sources_main_file: my_main_file.py\n"
        "    # Optional: Manually specify the name of the main file of the application, located in one of the 'sources' defined.\n"
        "    # Relative paths from a 'sources' entry, or absolute paths can be used.\n"
        "  data_persistence: False\n"
        "    # True to include all input and output files of the application in the resulting crate.\n"
        "    # If False, input and output files of the application won't be included, just referenced. False by default or if not set.\n"
        "  inputs: [/abs_path_to/dir_1, rel_path_to/dir_2, file_1, rel_path/file_2]\n"
        "    # Optional: Manually specify the inputs of the workflow. Relative or absolute paths can be used.\n"
        "  outputs: [/abs_path_to/dir_1, rel_path_to/dir_2, file_1, rel_path/file_2]\n"
        "    # Optional: Manually specify the outputs of the workflow. Relative or absolute paths can be used.\n"
        "  software:\n"
        "    # Optional: Manually specify the software dependencies of the application\n"
        "    - name: Software_1\n"
        "      version: Software_1 version description string\n"
        "      url: https://software_1.com/\n"
        "    - name: Software_2\n"
        "      version: Software_2 version description string\n"
        "      url: https://software_2.com/\n"
        "\n"
        "Authors:\n"
        "  - name: Author_1 Name\n"
        "    e-mail: author_1@email.com\n"
        "    orcid: https://orcid.org/XXXX-XXXX-XXXX-XXXX\n"
        "    organisation_name: Institution_1 name\n"
        "    ror: https://ror.org/XXXXXXXXX\n"
        "      # Find them in ror.org\n"
        "  - name: Author_2 Name\n"
        "    e-mail: author2@email.com\n"
        "    orcid: https://orcid.org/YYYY-YYYY-YYYY-YYYY\n"
        "    organisation_name: Institution_2 name\n"
        "    ror: https://ror.org/YYYYYYYYY\n"
        "      # Find them in ror.org\n"
        "\n"
        "Agent:\n"
        "  name: Name\n"
        "  e-mail: agent@email.com\n"
        "  orcid: https://orcid.org/XXXX-XXXX-XXXX-XXXX\n"
        "  organisation_name: Agent Institution name\n"
        "  ror: https://ror.org/XXXXXXXXX\n"
        "    # Find them in ror.org\n"
    )

    compss_crate = ROCrate()

    # First, read values defined by user from ro-crate-info.yaml
    try:
        with open(INFO_YAML, "r", encoding="utf-8") as f_p:
            try:
                yaml_content = yaml.safe_load(f_p)
            except yaml.YAMLError as exc:
                print(exc)
                raise exc
    except IOError:
        with open("ro-crate-info_TEMPLATE.yaml", "w", encoding="utf-8") as f_t:
            f_t.write(yaml_template)
            print(
                f"PROVENANCE | ERROR: YAML file {INFO_YAML} not found in your working directory. A template"
                " has been generated in file ro-crate-info_TEMPLATE.yaml"
            )
        raise

    # Generate Root entity section in the RO-Crate
    compss_wf_info, author_list = root_entity(compss_crate, yaml_content, INFO_YAML)

    # Get mainEntity from COMPSs runtime log dataprovenance.log
    compss_ver, main_entity, out_profile = get_main_entities(compss_wf_info, INFO_YAML, DP_LOG)

    # Process set of accessed files, as reported by COMPSs runtime.
    # This must be done before adding the Workflow to the RO-Crate
    ins, outs = process_accessed_files(DP_LOG)

    # Add application source files to the RO-Crate, that will also be physically in the crate
    add_application_source_files(
        compss_crate, compss_wf_info, compss_ver, main_entity, out_profile, INFO_YAML, COMPLETE_GRAPH
    )

    # Add in and out files, not to be physically copied in the Crate by default

    # First, add to the lists any inputs or outputs defined by the user, in case they exist
    if "inputs" in compss_wf_info:
        ins = add_manual_datasets("inputs", compss_wf_info, ins)
    if "outputs" in compss_wf_info:
        outs = add_manual_datasets("outputs", compss_wf_info, outs)

    ins, outs = fix_in_files_at_out_dirs(ins, outs)

    # Merge lists to avoid duplication when detecting common_paths
    ins_and_outs = ins.copy() + outs.copy()
    ins_and_outs.sort()  # Put together shared paths between ins an outs

    # print(f"PROVENANCE DEBUG | List of ins and outs: {ins_and_outs}")

    # The list has at this point detected ins and outs, but also added any ins an outs defined by the user

    list_common_paths = []
    part_time = time.time()
    if (
        "data_persistence" in compss_wf_info
        and compss_wf_info["data_persistence"] is True
    ):
        persistence = True
        list_common_paths = get_common_paths(ins_and_outs)
    else:
        persistence = False

    fixed_ins = []  # ins are file://host/path/file, fixed_ins are crate_path/file
    for item in ins:
        fixed_ins.append(
            add_dataset_file_to_crate(
                compss_crate, item, persistence, list_common_paths
            )
        )
    print(
        f"PROVENANCE | RO-Crate adding input files TIME (Persistence: {persistence}): "
        f"{time.time() - part_time} s"
    )

    part_time = time.time()

    fixed_outs = []
    for item in outs:
        fixed_outs.append(
            add_dataset_file_to_crate(
                compss_crate, item, persistence, list_common_paths
            )
        )
    print(
        f"PROVENANCE | RO-Crate adding output files TIME (Persistence: {persistence}): "
        f"{time.time() - part_time} s"
    )

    # print(f"FIXED_INS: {fixed_ins}")
    # print(f"FIXED_OUTS: {fixed_outs}")
    # Register execution details using WRROC profile
    # Compliance with RO-Crate WorkflowRun Level 2 profile, aka. Workflow Run Crate
    run_uuid = wrroc_create_action(
        compss_crate, main_entity, author_list, fixed_ins, fixed_outs, yaml_content
    )

    # ro-crate-py does not deal with profiles
    # compss_crate.metadata.append_to(
    #     "conformsTo", {"@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0"}
    # )

    #  Code from runcrate https://github.com/ResearchObject/runcrate/blob/411c70da556b60ee2373fea0928c91eb78dd9789/src/runcrate/convert.py#L270
    profiles = []
    for proc in "process", "workflow":
        id_ = f"{PROFILES_BASE}/{proc}/{PROFILES_VERSION}"
        profiles.append(
            compss_crate.add(
                ContextEntity(
                    compss_crate,
                    id_,
                    properties={
                        "@type": "CreativeWork",
                        "name": f"{proc.title()} Run Crate",
                        "version": PROFILES_VERSION,
                    },
                )
            )
        )
    # In the future, this could go out of sync with the wroc
    # profile added by ro-crate-py to the metadata descriptor
    wroc_profile_id = (
        f"https://w3id.org/workflowhub/workflow-ro-crate/{WROC_PROFILE_VERSION}"
    )
    profiles.append(
        compss_crate.add(
            ContextEntity(
                compss_crate,
                wroc_profile_id,
                properties={
                    "@type": "CreativeWork",
                    "name": "Workflow RO-Crate",
                    "version": WROC_PROFILE_VERSION,
                },
            )
        )
    )
    compss_crate.root_dataset["conformsTo"] = profiles

    # Add Checksum algorithm and "environment" to context
    compss_crate.metadata.extra_contexts.append(
        "https://w3id.org/ro/terms/workflow-run"
    )

    # Debug
    # for e in compss_crate.get_entities():
    #    print(e.id, e.type)

    # Dump to file
    part_time = time.time()
    folder = "COMPSs_RO-Crate_" + run_uuid + "/"
    sys.stdout.flush()  # All pending stdout to the log file
    compss_crate.write(folder)
    print(f"PROVENANCE | RO-Crate writing to disk TIME: {time.time() - part_time} s")
    print(
        f"PROVENANCE | Workflow Provenance generation TOTAL EXECUTION TIME: {time.time() - exec_time} s"
    )
    print(
        f"PROVENANCE | COMPSs Workflow Provenance successfully generated in sub-folder:\n\t{folder}"
    )


if __name__ == "__main__":

    # Usage: python /path_to/generate_COMPSs_RO-Crate.py ro-crate-info.yaml /path_to/dataprovenance.log
    if len(sys.argv) != 3:
        print(
            "PROVENANCE | Usage: python /path_to/generate_COMPSs_RO-Crate.py "
            "/path_to/your_info.yaml /path_to/dataprovenance.log"
        )
        sys.exit()
    else:
        INFO_YAML = sys.argv[1]
        DP_LOG = sys.argv[2]
        path_dplog = Path(sys.argv[2])
        COMPLETE_GRAPH = path_dplog.parent / "monitor/complete_graph.svg"
    main()
