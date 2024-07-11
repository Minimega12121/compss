import typing

from rocrate.rocrate import ROCrate
from rocrate.model.contextentity import ContextEntity


def set_profile_details(compss_crate: ROCrate) -> None:
    """
    Set all the details of the profiles used inside the RO-Crate

    :param compss_crate: The COMPSs RO-Crate being generated

    :returns: None
    """

    PROFILES_BASE = "https://w3id.org/ro/wfrun"
    PROFILES_VERSION = "0.5"
    WROC_PROFILE_VERSION = "1.0"

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
