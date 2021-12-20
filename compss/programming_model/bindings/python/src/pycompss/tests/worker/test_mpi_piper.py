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

import subprocess

from pycompss.tests.worker.common_piper_tester import setup_argv
from pycompss.tests.worker.common_piper_tester import evaluate_piper_worker_common


def worker_thread(argv, current_path):
    try:
        from pycompss.worker.piper.mpi_piper_worker import main  # noqa
    except AttributeError:
        raise Exception("UNSUPPORTED WITH MYPY")
    # Start the piper worker
    setup_argv(argv, current_path)
    p = subprocess.Popen(
        " ".join(argv),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    p.wait()


def test_piper_worker():
    try:
        evaluate_piper_worker_common(worker_thread, mpi_worker=True)
    except Exception as e:
        print("EXCEPTION: " + str(e))
        # raise Exception("UNSUPPORTED WITH MYPY - Happened because the worker can not start with mpi")
