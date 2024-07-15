import os
import typing

from pathlib import Path

from rocrate.rocrate import ROCrate
from rocrate.model.person import Person
from rocrate.model.contextentity import ContextEntity


def add_person_definition(
    compss_crate: ROCrate, contact_type: str, yaml_author: dict, info_yaml: str
) -> bool:
    """
    Check if a specified person has enough defined terms to be added in the RO-Crate.

    :param compss_crate: The COMPSs RO-Crate being generated
    :param contact_type: contactType definition for the ContactPoint. "Author" or "Agent"
    :param yaml_author: Content of the YAML file describing the user
    :param info_yaml: Name of the YAML file specified by the user

    :returns: If the person is valid. It is added if True
    """

    # Expected Person fields
    # orcid - Mandatory in RO-Crate 1.1
    # name - Mandatory in WorkflowHub
    # e-mail - Optional
    #
    # ror - Optional
    # organisation_name - Optional even if ror is defined. But won't show anything at WorkflowHub

    person_dict = {}
    mail_dict = {}
    org_dict = {}

    if not "orcid" in yaml_author:
        print(
            f"PROVENANCE | ERROR in your {info_yaml} file. A 'Person' is ignored, since it has no 'orcid' defined"
        )
        return False
    if "name" in yaml_author:
        person_dict["name"] = yaml_author["name"]
    # Name is no longer mandatory in WFHub
    # else:
    #     print(f"PROVENANCE | ERROR in your {info_yaml} file. A 'Person' is ignored, since it has 'orcid' but no 'name' defined")
    #     return False
    if "e-mail" in yaml_author:
        person_dict["contactPoint"] = {"@id": "mailto:" + yaml_author["e-mail"]}
        mail_dict["@type"] = "ContactPoint"
        mail_dict["contactType"] = contact_type
        mail_dict["email"] = yaml_author["e-mail"]
        mail_dict["identifier"] = yaml_author["e-mail"]
        mail_dict["url"] = yaml_author["orcid"]
        compss_crate.add(
            ContextEntity(compss_crate, "mailto:" + yaml_author["e-mail"], mail_dict)
        )
    if "ror" in yaml_author:
        person_dict["affiliation"] = {"@id": yaml_author["ror"]}
        # If ror defined, organisation_name becomes mandatory, if it is to be shown in WorkflowHub
        org_dict["@type"] = "Organization"
        if "organisation_name" in yaml_author:
            org_dict["name"] = yaml_author["organisation_name"]
            compss_crate.add(ContextEntity(compss_crate, yaml_author["ror"], org_dict))
        else:
            print(
                f"PROVENANCE | WARNING in your {info_yaml} file. 'organisation_name' not defined for an 'Organisation'"
            )
    compss_crate.add(Person(compss_crate, yaml_author["orcid"], person_dict))

    return True


def root_entity(
    compss_crate: ROCrate, yaml_content: dict, info_yaml: str
) -> typing.Tuple[dict, list]:
    """
    Generate the Root Entity in the RO-Crate generated for the COMPSs application

    :param compss_crate: The COMPSs RO-Crate being generated
    :param yaml_content: Content of the YAML file specified by the user
    :param info_yaml: Name of the YAML file specified by the user

    :returns: 'COMPSs Workflow Information' and 'Authors' sections, as defined in the YAML
    """

    # Get Sections
    compss_wf_info = yaml_content["COMPSs Workflow Information"]
    authors_info = []
    if "Authors" in yaml_content:
        authors_info_yaml = yaml_content["Authors"]  # Now a list of authors
        if isinstance(authors_info_yaml, list):
            authors_info = authors_info_yaml
        else:
            authors_info.append(authors_info_yaml)

    # COMPSs Workflow RO Crate generation
    # Root Entity

    # SHOULD in RO-Crate 1.1. MUST in WorkflowHub
    compss_crate.name = compss_wf_info["name"]

    if "description" in compss_wf_info:
        # SHOULD in Workflow Profile and WorkflowHub
        compss_crate.description = compss_wf_info["description"]

    if "license" in compss_wf_info:
        # License details could be also added as a Contextual Entity. MUST in Workflow RO-Crate Profile, but WorkflowHub does not consider it a mandatory field
        compss_crate.license = compss_wf_info["license"]

    author_list = []
    org_list = []

    for author in authors_info:
        if "orcid" in author and author["orcid"] in author_list:
            break
        if add_person_definition(compss_crate, "Author", author, info_yaml):
            author_list.append(author["orcid"])
            if "ror" in author and author["ror"] not in org_list:
                org_list.append(author["ror"])

    # Generate 'creator' and 'publisher' terms
    crate_author_list = []
    crate_org_list = []
    for author_orcid in author_list:
        crate_author_list.append({"@id": author_orcid})
    if crate_author_list:
        compss_crate.creator = crate_author_list
    for org_ror in org_list:
        crate_org_list.append({"@id": org_ror})

    # publisher is SHOULD in RO-Crate 1.1. Preferably an Organisation, but could be a Person
    if not crate_org_list:
        # Empty list of organisations, add authors as publishers
        if crate_author_list:
            compss_crate.publisher = crate_author_list
    else:
        compss_crate.publisher = crate_org_list

    if len(crate_author_list) == 0:
        print(f"PROVENANCE | WARNING: No valid 'Authors' specified in {info_yaml}")

    return compss_wf_info, crate_author_list


def get_main_entities(
    wf_info: dict, info_yaml: str, dp_log: str
) -> typing.Tuple[str, str, str]:
    """
    Get COMPSs version and mainEntity from dataprovenance.log first lines
    3 First lines expected format: compss_version_number\n main_entity\n output_profile_file\n
    Next lines are for "accessed files" and "direction"
    mainEntity can be directly obtained for Python, or defined by the user in the YAML (sources_main_file)

    :param wf_info: YAML dict to extract info form the application, as specified by the user
    :param info_yaml: Name of the YAML file specified by the user
    :param dp_log: Full path to the dataprovenance.log file

    :returns: COMPSs version, main COMPSs file name, COMPSs profile file name
    """

    # Build the whole source files list in list_of_sources, and get a backup main entity, in case we can't find one
    # automatically. The mainEntity must be an existing file, otherwise the RO-Crate won't have a ComputationalWorkflow
    yaml_sources_list = []  # YAML sources list
    list_of_sources = []  # Full list of source files, once directories are traversed
    # Should contain absolute paths, for correct comparison (two files in different directories
    # could be named the same)

    main_entity = None
    backup_main_entity = None

    if "sources" in wf_info:
        if isinstance(wf_info["sources"], list):
            yaml_sources_list.extend(wf_info["sources"])
        else:
            yaml_sources_list.append(wf_info["sources"])
    if "files" in wf_info:
        # Backward compatibility: if old "sources_dir" and "files" have been used, merge in yaml_sources_list.
        if isinstance(wf_info["files"], list):
            yaml_sources_list.extend(wf_info["files"])
        else:
            yaml_sources_list.append(wf_info["files"])
    if "sources_dir" in wf_info:
        #  Backward compatibility: if old "sources_dir" and "files" have been used, merge in yaml_sources_list.
        # sources_list = list(tuple(wf_info["files"])) + list(tuple(wf_info["sources"]))
        if isinstance(wf_info["sources_dir"], list):
            yaml_sources_list.extend(wf_info["sources_dir"])
        else:
            yaml_sources_list.append(wf_info["sources_dir"])

    keys = ["sources", "files", "sources_dir"]
    if not any(key in wf_info for key in keys):
        # If no sources are defined, define automatically the main_entity or return error
        # We try directly to add the mainEntity identified in dataprovenance.log, if exists in the CWD
        with open(dp_log, "r", encoding="UTF-8") as dp_file:
            compss_v = next(dp_file).rstrip()  # First line, COMPSs version number
            second_line = next(dp_file).rstrip()
            # Second, main_entity. Use better rstrip, just in case there is no '\n'
            if second_line.endswith(".py"):
                # Python. Line contains only the file name, need to locate it
                detected_app = second_line
            else:  # Java app. Need to fix filename first
                # Translate identified main entity matmul.files.Matmul to a comparable path
                me_file_name = second_line.split(".")[-1]
                detected_app = me_file_name + ".java"
            # print(f"PROVENANCE DEBUG | Detected app when no 'sources' defined is: {detected_app}")
            third_line = next(dp_file).rstrip()
            out_profile_fn = Path(third_line)
        if os.path.isfile(detected_app):
            main_entity = detected_app
        else:
            print(
                f"PROVENANCE | ERROR: No 'sources' defined at {info_yaml}, and detected 'mainEntity' not found in Current Working Directory"
            )
            raise KeyError(f"No 'sources' key defined at {info_yaml}")

    # Find a backup_main_entity while building the full list of source files
    for source in yaml_sources_list:
        path_source = Path(source).expanduser()
        resolved_source = str(path_source.resolve())
        if path_source.exists():
            if os.path.isfile(resolved_source):
                list_of_sources.append(resolved_source)
                if backup_main_entity is None and path_source.suffix in {
                    ".py",
                    ".java",
                    ".jar",
                    ".class",
                }:
                    backup_main_entity = resolved_source
                    # print(
                    #     f"PROVENANCE DEBUG | FOUND SOURCE FILE AS BACKUP MAIN: {backup_main_entity}"
                    # )
            elif os.path.isdir(resolved_source):
                for root, _, files in os.walk(
                    resolved_source, topdown=True, followlinks=True
                ):
                    if "__pycache__" in root:
                        continue  # We skip __pycache__ subdirectories
                    for f_name in files:
                        # print(f"PROVENANCE DEBUG | ADDING FILE to list_of_sources: {f_name}. root is: {root}")
                        if f_name.startswith("*"):
                            # Avoid dealing with symlinks with wildcards
                            continue
                        full_name = os.path.join(root, f_name)
                        list_of_sources.append(full_name)
                        if backup_main_entity is None and Path(f_name).suffix in {
                            ".py",
                            ".java",
                            ".jar",
                            ".class",
                        }:
                            backup_main_entity = full_name
                            # print(
                            #     f"PROVENANCE DEBUG | FOUND SOURCE FILE IN A DIRECTORY AS BACKUP MAIN: {backup_main_entity}"
                            # )
            else:
                print(
                    f"PROVENANCE | WARNING: A defined source is neither a directory, nor a file ({resolved_source})"
                )
        else:
            print(
                f"PROVENANCE | WARNING: Specified file or directory in {info_yaml} 'sources' does not exist ({path_source})"
            )

    # Can't get backup_main_entity from sources_main_file, because we do not know if it really exists
    if len(list_of_sources) == 0:
        print(
            "PROVENANCE | WARNING: Unable to find application source files. Please, review your "
            "ro_crate_info.yaml definition ('sources' term)"
        )
        # raise FileNotFoundError
    elif backup_main_entity is None:
        # No source files found in list_of_sources, set any file as backup
        backup_main_entity = list_of_sources[0]

    # print(f"PROVENANCE DEBUG | backup_main_entity is: {backup_main_entity}")

    with open(dp_log, "r", encoding="UTF-8") as dp_file:
        compss_v = next(dp_file).rstrip()  # First line, COMPSs version number
        second_line = next(dp_file).rstrip()
        # Second, main_entity. Use better rstrip, just in case there is no '\n'
        if second_line.endswith(".py"):
            # Python. Line contains only the file name, need to locate it
            detected_app = second_line
        else:  # Java app. Need to fix filename first
            # Translate identified main entity matmul.files.Matmul to a comparable path
            me_sub_path = second_line.replace(".", "/")
            detected_app = me_sub_path + ".java"
        # print(f"PROVENANCE DEBUG | Detected app is: {detected_app}")
        third_line = next(dp_file).rstrip()
        out_profile_fn = Path(third_line)

    for file in list_of_sources:  # Try to find the identified mainEntity
        if file.endswith(detected_app):
            # print(
            #     f"PROVENANCE DEBUG | IDENTIFIED MAIN ENTITY FOUND IN LIST OF FILES: {file}"
            # )
            main_entity = file
            break
    # main_entity has a value if mainEntity has been automatically detected

    if "sources_main_file" in wf_info:
        # Check what the user has defined
        # If it directly exists, we are done, no need to search in 'sources'
        found = False
        path_smf = Path(wf_info["sources_main_file"]).expanduser()
        resolved_sources_main_file = str(path_smf.resolve())
        if os.path.isfile(path_smf):
            # Checks if exists
            if main_entity is None:
                # the detected_app was not found previously in the list of files
                found = True
                print(
                    f"PROVENANCE | WARNING: The file defined at sources_main_file is assigned as 'mainEntity': {resolved_sources_main_file}"
                )
            else:
                print(
                    f"PROVENANCE | WARNING: The file defined at sources_main_file "
                    f"({resolved_sources_main_file}) in {info_yaml} does not match with the "
                    f"automatically identified 'mainEntity' ({main_entity})"
                )
            main_entity = resolved_sources_main_file
            found = True
        else:
            # If the file defined in sources_main_file is not directly found, try to find it in 'sources'
            # if sources_main_file is an absolute path, the join has no effect
            for source in yaml_sources_list:  # Created at the beginning
                path_sources = Path(source).expanduser()
                if not path_sources.exists() or os.path.isfile(source):
                    continue
                resolved_sources = str(path_sources.resolve())
                resolved_sources_main_file = os.path.join(
                    resolved_sources, wf_info["sources_main_file"]
                )
                for file in list_of_sources:
                    if file == resolved_sources_main_file:
                        # The file exists
                        # print(
                        #     f"PROVENANCE DEBUG | The file defined at sources_main_file exists: "
                        #     f" {resolved_sources_main_file}"
                        # )
                        if resolved_sources_main_file != main_entity:
                            print(
                                f"PROVENANCE | WARNING: The file defined at sources_main_file "
                                f"({resolved_sources_main_file}) in {info_yaml} does not match with the "
                                f"automatically identified 'mainEntity' ({main_entity})"
                            )
                        # else: the user has defined exactly the file we found
                        # In both cases: set file defined by user
                        main_entity = resolved_sources_main_file
                        # Can't use Path, file may not be in cwd
                        found = True
                        break
                    if file.endswith(wf_info["sources_main_file"]):
                        # The file exists
                        # print(
                        #     f"PROVENANCE DEBUG | The file defined at sources_main_file exists: "
                        #     f" {resolved_sources_main_file}"
                        # )
                        if file != main_entity:
                            print(
                                f"PROVENANCE | WARNING: The file defined at sources_main_file "
                                f"({file}) in {info_yaml} does not match with the "
                                f"automatically identified 'mainEntity' ({main_entity})"
                            )
                        # else: the user has defined exactly the file we found
                        # In both cases: set file defined by user
                        main_entity = file
                        # Can't use Path, file may not be in cwd
                        found = True
                        break
            if not found:
                print(
                    f"PROVENANCE | WARNING: the defined 'sources_main_file' ({wf_info['sources_main_file']}) does "
                    f"not exist in the defined 'sources'. Check your {info_yaml}."
                )
                # If we identified the mainEntity automatically, we select it when the one defined
                # by the user is not found

    if main_entity is None:
        # When neither identified, nor defined by user: get backup if exists
        if backup_main_entity is None:
            # We have a fatal problem
            print(
                f"PROVENANCE | ERROR: no 'mainEntity' has been found. Check the definition of 'sources' and "
                f"'sources_main_file' in {info_yaml}"
            )
            raise FileNotFoundError
        main_entity = backup_main_entity
        print(
            f"PROVENANCE | WARNING: the detected 'mainEntity' {detected_app} does not exist in the list "
            f"of application files provided in {info_yaml}. Setting {main_entity} as mainEntity"
        )

    print(
        f"PROVENANCE | COMPSs version: '{compss_v}', out_profile: '{out_profile_fn.name}', main_entity: '{main_entity}'"
    )

    return compss_v, main_entity, out_profile_fn.name


def get_manually_defined_software_requirements(
    compss_crate: ROCrate, wf_info: dict, info_yaml: str
) -> list:
    """
    Extract all application software dependencies manually specified by the user in the YAML file. At least "name"
    and "version" must be specified in the YAML

    :param compss_crate: The COMPSs RO-Crate being generated
    :param wf_info: YAML dict to extract info form the application, as specified by the user
    :param info_yaml: Name of the YAML file specified by the user

    :returns: list of id's to be added to the ComputationalWorkflow as softwareRequirements
    """

    software_info = []
    software_requirements_list = []

    if not "software" in wf_info:
        return None

    if isinstance(wf_info["software"], list):
        software_info = wf_info["software"]
    else:
        software_info.append(wf_info["software"])

    for soft_details in software_info:
        if not "name" in soft_details or not "version" in soft_details:
            print(
                f"PROVENANCE | WARNING in your {info_yaml} file. A 'software' does not have a 'name' or 'version' "
                f"defined. The 'software' dependency definition will be ignored"
            )
            continue
        software_dict = {"@type": "SoftwareApplication"}
        if "url" in soft_details:
            software_id = soft_details["url"]
            software_dict["url"] = soft_details["url"]
        else:
            software_id = "#" + soft_details["name"].lower()
        software_dict["name"] = soft_details["name"]
        software_dict["version"] = soft_details["version"]
        software_requirements_list.append({"@id": software_id})
        compss_crate.add(ContextEntity(compss_crate, software_id, software_dict))
        print(
            f"PROVENANCE | 'softwareRequirements' dependency correctly added: {soft_details['name']}"
        )

    return software_requirements_list
