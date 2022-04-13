#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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

# -*- coding: utf-8 -*-

"""
PyCOMPSs Util - Interactive - Mode Flags checker.

Provides auxiliary methods for the interactive mode flags checking
"""

from pycompss.util.typing_helper import typing

# None type
NONE_TYPE = type(None)

# Required flags - Structure:
#   - key = flag name
#   - value = [ [supported types], [supported values]]
#       - supported values is optional
REQUIRED_FLAGS = {
    "log_level": [[str], ["trace", "debug", "info", "api", "off"]],
    "debug": [[bool]],
    "o_c": [[bool]],
    "graph": [[bool]],
    "trace": [[bool, str], [True, False, "scorep", "arm-map", "arm-ddt"]],
    "monitor": [[int, NONE_TYPE]],
    "project_xml": [[str, NONE_TYPE]],
    "resources_xml": [[str, NONE_TYPE]],
    "summary": [[bool]],
    "task_execution": [[str]],
    "storage_impl": [[str, NONE_TYPE]],
    "storage_conf": [[str, NONE_TYPE]],
    "streaming_backend": [[str, NONE_TYPE]],
    "streaming_master_name": [[str, NONE_TYPE]],
    "streaming_master_port": [[str, NONE_TYPE]],
    "task_count": [[int]],
    "app_name": [[str]],
    "uuid": [[str, NONE_TYPE]],
    "base_log_dir": [[str, NONE_TYPE]],
    "specific_log_dir": [[str, NONE_TYPE]],
    "extrae_cfg": [[str, NONE_TYPE]],
    "comm": [[str], ["NIO", "GAT"]],
    "conn": [
        [str],
        [
            "es.bsc.compss.connectors.DefaultSSHConnector",
            "es.bsc.compss.connectors.DefaultNoSSHConnector",
        ],
    ],
    "master_name": [[str]],
    "master_port": [[str]],
    "scheduler": [[str]],
    "jvm_workers": [[str]],
    "cpu_affinity": [[str]],
    "gpu_affinity": [[str]],
    "fpga_affinity": [[str]],
    "fpga_reprogram": [[str]],
    "profile_input": [[str]],
    "profile_output": [[str]],
    "scheduler_config": [[str]],
    "external_adaptation": [[bool]],
    "propagate_virtual_environment": [[bool]],
    "mpi_worker": [[bool]],
}


def check_flags(all_vars: dict) -> typing.Tuple[bool, list]:
    """Check that the provided flags are supported.

    :param all_vars: Flags dictionary.
    :return: If all flags are supported and the issues if exists.
    """
    is_ok = True
    issues = []
    flags = all_vars.keys()

    missing_flags = [key for key in REQUIRED_FLAGS.keys() if key not in flags]
    if missing_flags:
        # There are missing flags
        is_ok = False
        for missing_flag in missing_flags:
            issues.append("Missing flag: %s" % missing_flag)
        return is_ok, issues
    else:
        # Check that each element is of the correct type and supported value
        for flag, requirements in REQUIRED_FLAGS.items():
            issues += __check_flag__(all_vars, flag, requirements)
        if issues:
            is_ok = False

    return is_ok, issues


def __check_flag__(all_vars: dict, flag: str, requirements: typing.Any) -> list:
    """Check the given flag against the requirements looking for issues.

    :param all_vars: All variables.
    :param flag: Flag to check.
    :param requirements: Flag requirements.
    :returns: A list of issues (empty if none).
    """
    flag_header = "Flag "
    is_not = " is not "
    issues = []
    if len(requirements) == 1:
        # Only check type
        req_type = requirements[0]
        if not type(all_vars[flag]) in req_type:
            issues.append(flag_header + flag + is_not + str(req_type))
    if len(requirements) == 2:
        # Check type
        req_type = requirements[0]
        req_values = requirements[1]
        if not type(all_vars[flag]) in req_type:
            issues.append(flag_header + flag + is_not + str(req_type))
        else:
            # Check that it is also one of the options
            if not all_vars[flag] in req_values:
                issues.append(
                    flag_header
                    + flag
                    + "="
                    + all_vars[flag]
                    + " is not supported. Available values: "
                    + str(req_values)
                )
    return issues


def print_flag_issues(issues: list) -> None:
    """Display the given issues on stdout.

    :param issues: Flag issues.
    :return: None.
    """
    print("[ERROR] The following flag issues were detected:")
    for issue in issues:
        print("\t - %s" % issue)
