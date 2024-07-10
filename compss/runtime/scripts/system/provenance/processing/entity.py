import typing

from rocrate.rocrate import ROCrate
from rocrate.model.person import Person
from rocrate.model.contextentity import ContextEntity

def add_person_definition(
    compss_crate: ROCrate,
    contact_type: str,
    yaml_author: dict,
    info_yaml: str
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
    #     print(f"PROVENANCE | ERROR in your {INFO_YAML} file. A 'Person' is ignored, since it has 'orcid' but no 'name' defined")
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
                f"PROVENANCE | WARNING in your {INFO_YAML} file. 'organisation_name' not defined for an 'Organisation'"
            )
    compss_crate.add(Person(compss_crate, yaml_author["orcid"], person_dict))

    return True

def root_entity(compss_crate: ROCrate, yaml_content: dict, info_yaml: str) -> typing.Tuple[dict, list]:
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