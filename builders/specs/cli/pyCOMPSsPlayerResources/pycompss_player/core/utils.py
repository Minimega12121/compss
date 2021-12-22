import json
from glob import glob
from pathlib import Path
import subprocess

def get_object_method_by_name(obj, method_name, include_in_name=False):
    for class_method_name in dir(obj):
        if not '__' in class_method_name and callable(getattr(obj, class_method_name)):
            if class_method_name.startswith(method_name) or (include_in_name and method_name in class_method_name):
                return class_method_name

def table_print(col_names, data):
    row_format ="{:>15}" * (len(col_names) + 1)
    print(row_format.format("", *col_names))
    for row in data:
        print(row_format.format('-', *row))

def get_current_env(return_path=False):
    home_path = str(Path.home())
    current_env = glob(home_path + '/.COMPSs/envs/*/current')[0].replace('current', 'env.json')
    with open(current_env, 'r') as env:
        if return_path:
            return json.load(env), current_env
        return json.load(env)

def ssh_run_commands(login_info, commands, **kwargs):
    cmd = ' ; '.join(commands)
    res = subprocess.run(f"ssh {login_info} '{cmd}'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
    return res.stdout.decode()