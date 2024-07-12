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
import pycompss_cli.core.utils as utils
import subprocess, os, shutil
from typing import List
from rocrate.rocrate import ROCrate
from datetime import datetime

from pycompss_cli.core.cmd_helpers import command_runner

# ################ #
# GLOBAL VARIABLES #
# ################ #

# ############# #
# API FUNCTIONS #
# ############# #

def local_deploy_compss(working_dir: str = "") -> None:
    """ Starts the main COMPSs image in Docker.
    It stops any existing one since it can not coexist with itself.

    :param working_dir: Given working directory
    :param image: Given docker image
    :param restart: Force stop the existing and start a new one.
    :returns: None
    """
    
    # cfg_content = '{"working_dir":"' + working_dir + \
    #                 '","resources":"","project":""}'
    # tmp_path, cfg_file = _store_temp_cfg(cfg_content)
    # _copy_file(cfg_file, default_cfg)
    # shutil.rmtree(tmp_path)
    pass


def local_run_app(cmd: List[str]) -> None:
    """ Execute the given command in the main COMPSs image in Docker.

    :param cmd: Command to execute.
    :returns: The execution stdout.
    """

    if utils.check_exit_code('which enqueue_compss') == 1:
        cmd = ['module load COMPSs'] + cmd
    cmd = ';'.join(cmd)

    subprocess.run(cmd, shell=True)


def local_jupyter(work_dir, lab_or_notebook, jupyter_args):
    cmd = f'jupyter {lab_or_notebook} --notebook-dir=' + work_dir
    process = subprocess.Popen(cmd + ' ' + jupyter_args, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        while True:
            line = process.stdout.readline()
            if line == '' and process.poll() is not None:
                break
            if line:
                print(line.strip().decode('utf-8'))
            if process.poll() is not None:
                break
    except KeyboardInterrupt:
        print('Closing jupyter...')
        process.kill()
    

def local_exec_app(command, return_process=False):
    p = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if return_process:
        return p
    return p.stdout.decode().strip()


def local_submit_job(app_args, env_vars):
    cmd = f'enqueue_compss {app_args}'
    if utils.check_exit_code('which enqueue_compss') == 1:
        cmd = 'module load COMPSs;' + cmd
        
    if env_vars:
        cmd = ' ; '.join([*[f'export {var}' for var in env_vars], cmd])

    p = local_exec_app(cmd, return_process=True)
    job_id = p.stdout.decode().strip().split('\n')[-1].split(' ')[-1]
    if p.returncode != 0:
        print('ERROR:', p.stderr.decode())
    else:
        print('Job submitted:', job_id)
        return job_id

def local_job_list(local_job_scripts_dir):
    cmd = f"python3 {local_job_scripts_dir}/find.py"
    if utils.check_exit_code('which enqueue_compss') == 1:
        cmd = 'module load COMPSs;' + cmd
    return local_exec_app(cmd)

def local_cancel_job(local_job_scripts_dir, jobid):
    cmd = f"python3 {local_job_scripts_dir}/cancel.py {jobid}"
    if utils.check_exit_code('which enqueue_compss') == 1:
        cmd = 'module load COMPSs;' + cmd
    return local_exec_app(cmd)

def local_job_status(local_job_scripts_dir, jobid):
    cmd = f"python3 {local_job_scripts_dir}/status.py {jobid}"
    if utils.check_exit_code('which enqueue_compss') == 1:
        cmd = 'module load COMPSs;' + cmd
    status = local_exec_app(cmd)

    if status == 'SUCCESS\nSTATUS:':
        return 'ERROR'
    return status

def local_app_deploy(local_source: str, app_dir: str, dest_dir: str = None):
    dst = os.path.abspath(dest_dir) if dest_dir else app_dir
    os.makedirs(dst, exist_ok=True)
    for f in os.listdir(local_source):
        if os.path.isfile(os.path.join(local_source, f)):
            shutil.copy(os.path.join(local_source, f), os.path.join(dst, f))
        else:
            shutil.copytree(os.path.join(local_source, f), os.path.join(dst, f))
    if dest_dir is not None:
        with open(app_dir + '/.compss', 'w') as f:
            f.write(dst)
    print('App deployed from ' + local_source + ' to ' + dst)

def local_inspect(ro_crate_zip_or_dir: str):
    try:
        crate = ROCrate(ro_crate_zip_or_dir)
    except Exception as e:
        print(f"Error al cargar el RO-Crate: {e}")
        raise

    pointers = ["├── ", "└── "]
    follow_prefix = "│   "
    empty_prefix = "    "

    print(f"================================================================================")
    print(f"{ro_crate_zip_or_dir}")

    prefix = follow_prefix
    profiles = []
    for e in crate.get_entities():
        if e.id == "./":
            publish_time = datetime.fromisoformat(e.get('datePublished'))
            print(pointers[0] + "Date Published")
            print(prefix + pointers[1] + f"{publish_time.strftime('%A, %d of %B of %Y - %H:%M %Z')}")
            print(pointers[0] + "Name")
            print(prefix + pointers[1] + e.get('name'))
            print(pointers[0] + "Authors")
            creators = e.get('creator')
            if creators:
                for i, c in enumerate(creators):
                    if i == (len(creators) - 1):
                        print(prefix + pointers[1] + c['name'])
                    else:
                        print(prefix + pointers[0] + c['name'])
            desc_str = e.get('description')
            print(pointers[0] + "License")
            print(prefix + pointers[1] + e.get('license'))
        elif e.type == "CreativeWork":
            if e.id.startswith("https"):
                profiles.append(e['name'] + " (" + e['version'] + ")")
        elif e.id == "#compss":
            print(pointers[0] + "COMPSs Runtime version")
            print(prefix + pointers[1] + e.get('version'))
        elif "ComputationalWorkflow" in e.type:
            print(pointers[0] + "Software Dependencies")
            software_requirements = e.get("softwareRequirements")
            if software_requirements:
                if isinstance(software_requirements, list):
                    for i, s in enumerate(software_requirements):
                        if i == (len(software_requirements) - 1):
                            print(prefix + pointers[1] + s['name'])
                        else:
                            print(prefix + pointers[0] + s['name'])
                else:
                    print(prefix + pointers[1] + software_requirements['name'])
    print(pointers[0] + "RO-Crate Profiles compliance")
    for i, prof in enumerate(profiles):
        if i == (len(profiles) - 1):
            print(prefix + pointers[1] + prof)
        else:
            print(prefix + pointers[0] + prof)
    print(pointers[0] + "Description")
    print(prefix + pointers[1] + desc_str)


    prefix = empty_prefix + follow_prefix
    for e in crate.get_entities():
        if e.type == "CreateAction":
            print(pointers[1] + "CreateAction (execution details)")
            print(empty_prefix + pointers[0] + "Agent")
            agent_entity = e.get('agent')
            if agent_entity:
                print(prefix + pointers[1] + agent_entity['name'])
            print(empty_prefix + pointers[0] + "Application's main file")
            print(prefix + pointers[1] + e.get('instrument')['name'])
            # Parse 'name' for hostname and JOB_ID
            # "COMPSs cch_matmul_test.py execution at bsc_nvidia with JOB_ID 1930225"
            exec_info = e.get('name').split(' ')
            print(empty_prefix + pointers[0] + "Hostname")
            print(prefix + pointers[1] + exec_info[4])
            if len(exec_info) == 8:
                print(empty_prefix + pointers[0] + "Job ID")
                print(prefix + pointers[1] + exec_info[7])

            # Environment
            description = e.get('description')
            print(empty_prefix + pointers[0] + "Description (machine details)")
            print(prefix + pointers[1] + description)
            environment = e.get('environment')
            env_list = []
            if environment:
                for env in environment:
                    env_list.append((env.get('name'), env.get('value')))
                print(empty_prefix + pointers[0] + "Environment")
                for i, env_item in enumerate(env_list):
                    if i == (len(env_list) - 1):
                        print(prefix + pointers[1] + f"{env_item[0]} = {env_item[1]}")
                    else:
                        print(prefix + pointers[0] + f"{env_item[0]} = {env_item[1]}")

            # Times
            e_startTime = e.get('startTime')
            if e_startTime:
                startTime = datetime.fromisoformat(e_startTime)
                print(empty_prefix + pointers[0] + "Start Time")
                print(prefix + pointers[1] + f"{startTime.strftime('%A, %d of %B of %Y - %H:%M:%S %Z')}")
            endTime = datetime.fromisoformat(e.get('endTime'))
            print(empty_prefix + pointers[0] + "End Time")
            print(prefix + pointers[1] + f"{endTime.strftime('%A, %d of %B of %Y - %H:%M:%S %Z')}")
            # total_time = datetime.fromisoformat(endTime) - datetime.fromisoformat(startTime)
            if e_startTime:
                total_time = endTime - startTime
                print(empty_prefix + pointers[0] + "TOTAL EXECUTION TIME")
                print(prefix + pointers[1] + f"{total_time} s")

            # The 'object' list in the JSON can contain "File" objects, but also strings referencing remote files
            # wf_inputs = e.get('object')
            # inputs_list = []
            # for i, wf_in in enumerate(wf_inputs):
            #     if isinstance(wf_in, File):
            #         name = wf_in.get('name')
            #     else:
            #         name = wf_in
            #     print(f"Name: {name}")
            #     inputs_list.append(name)
            # print(f"\tList of needed inputs: {inputs_list}")

            # Inputs and Outputs
            wf_inputs = e.get('object')
            if wf_inputs:
                if not e.get('result'):
                    prefix = 2 * empty_prefix
                    print(empty_prefix + pointers[1] + "INPUTS")
                else:
                    print(empty_prefix + pointers[0] + "INPUTS")
                for (i, wf_in) in enumerate(wf_inputs):
                    if i == (len(wf_inputs) - 1):
                        print(prefix + pointers[1] + f"{wf_in.get('@id')}")
                    else:
                        print(prefix + pointers[0] + f"{wf_in.get('@id')}")

            wf_outputs = e.get('result')
            if wf_outputs:
                prefix = 2 * empty_prefix
                print(empty_prefix + pointers[1] + "OUTPUTS")
                for (i, wf_out) in enumerate(wf_outputs):
                    if i == (len(wf_outputs) - 1):
                        print(prefix + pointers[1] + f"{wf_out.get('@id')}")
                    else:
                        print(prefix + pointers[0] + f"{wf_out.get('@id')}")

    print(f"================================================================================")

    # meta = crate.dereference("ro-crate-metadata.json")
    # print(meta["conformsTo"])
