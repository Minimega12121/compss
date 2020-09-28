/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.invokers.types;

import es.bsc.compss.types.execution.LanguageParams;


/**
 * Python related variables.
 */
public class PythonParams implements LanguageParams {

    private String pythonInterpreter;
    private String pythonVersion;
    private String pythonVirtualEnvironment;
    private String pythonPropagateVirtualEnvironment;
    private String pythonPath;
    private Boolean pythonMpiWorker;


    /**
     * Python related parameters constructor.
     * 
     * @param pythonInterpreter Python Interpreter
     * @param pythonVersion Python Version
     * @param pythonVirtualEnvironment Python Virtual Environment
     * @param pythonPropagateVirtualEnvironment Python Propagate Virtual Environment flag
     * @param pythonPath Python Path
     * @param pythonMpiWorker Python MPI Worker flag
     */
    public PythonParams(String pythonInterpreter, String pythonVersion, String pythonVirtualEnvironment,
        String pythonPropagateVirtualEnvironment, String pythonPath, String pythonMpiWorker) {

        this.pythonInterpreter = pythonInterpreter;
        this.pythonVersion = pythonVersion;
        this.pythonVirtualEnvironment = pythonVirtualEnvironment;
        this.pythonPropagateVirtualEnvironment = pythonPropagateVirtualEnvironment;
        this.pythonPath = pythonPath.equals("null") ? "" : pythonPath;
        this.pythonMpiWorker = Boolean.parseBoolean(pythonMpiWorker);
    }

    public String getPythonInterpreter() {
        return this.pythonInterpreter;
    }

    public void setPythonInterpreter(String pythonInterpreter) {
        this.pythonInterpreter = pythonInterpreter;
    }

    public String getPythonVersion() {
        return this.pythonVersion;
    }

    public void setPythonVersion(String pythonVersion) {
        this.pythonVersion = pythonVersion;
    }

    public String getPythonVirtualEnvironment() {
        return this.pythonVirtualEnvironment;
    }

    public void setPythonVirtualEnvironment(String pythonVirtualEnvironment) {
        this.pythonVirtualEnvironment = pythonVirtualEnvironment;
    }

    public String getPythonPropagateVirtualEnvironment() {
        return this.pythonPropagateVirtualEnvironment;
    }

    public void setPythonPropagateVirtualEnvironment(String pythonPropagateVirtualEnvironment) {
        this.pythonPropagateVirtualEnvironment = pythonPropagateVirtualEnvironment;
    }

    public String getPythonPath() {
        return this.pythonPath;
    }

    public void setPythonPath(String pythonPath) {
        this.pythonPath = pythonPath;
    }

    public Boolean usePythonMpiWorker() {
        return this.pythonMpiWorker;
    }

    /**
     * Checks if python interpreter is a coverage stuff and converts to a normal coverage command string.
     * 
     * @return converted coverage command string.
     */
    public String checkCoverageAndGetPythonInterpreter() {
        // Checking if running in coverage mode
        if (this.pythonInterpreter.startsWith("coverage")) {
            return this.pythonInterpreter.replace("#", " ");
        } else {
            return this.pythonInterpreter;
        }
    }

}
