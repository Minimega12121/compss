import typing
import os
import uuid
import subprocess
import socket
import sys

from pathlib import Path
from datetime import timezone
from datetime import datetime
sys.path.append("..")
from utils.url_fixes import fix_dir_url
from processing.entities import add_person_definition

from rocrate.rocrate import ROCrate
from rocrate.model.contextentity import ContextEntity
from rocrate.utils import iso_now


def wrroc_create_action(
    compss_crate: ROCrate,
    main_entity: str,
    author_list: list,
    ins: list,
    outs: list,
    yaml_content: dict,
    info_yaml: str,
    dp_log: str
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
    :param info_yaml: Name of the YAML file specified by the user
    :param dp_log: Full path to the dataprovenance.log file

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
        compss_crate, "Agent", yaml_content["Agent"], info_yaml
    ):
        agent = {"@id": yaml_content["Agent"]["orcid"]}
    else:  # Choose first author, to avoid leaving it empty. May be true most of the times
        if author_list:
            agent = author_list[0]
            print(
                f"PROVENANCE | WARNING: 'Agent' not specified in {info_yaml}. First author selected by default."
            )
        else:
            agent = None
            print(
                f"PROVENANCE | WARNING: No 'Authors' or 'Agent' specified in {info_yaml}"
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
        with open(dp_log, "r", encoding="UTF-8") as dp_file:
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
