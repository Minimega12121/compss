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
package es.bsc.compss.local;

import es.bsc.compss.types.COMPSsMaster;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.CollectionParameter;
import es.bsc.compss.types.parameter.DictCollectionParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceDescription;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class LocalJob extends Job<COMPSsMaster> implements Invocation {

    private final List<LocalParameter> arguments;
    private LocalParameter target;
    private LinkedList<LocalParameter> results;
    private MethodResourceDescription reqs;
    private final List<String> slaveWorkersNodeNames;
    private TaskType taskType;


    /**
     * Creates a new LocalJob instance.
     * 
     * @param taskId Associated task Id.
     * @param task Associated task description.
     * @param impl Task implementation.
     * @param res Resource.
     * @param slaveWorkersNodeNames List of slave nodes for multi-node tasks.
     * @param listener Task listener.
     */
    public LocalJob(int taskId, TaskDescription task, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener) {

        super(taskId, task, impl, res, listener);
        this.taskType = impl.getTaskType();
        // Construct parameters
        final boolean hasTarget = this.taskParams.hasTargetObject();
        final int numReturns = this.taskParams.getNumReturns();
        this.arguments = new LinkedList<>();
        this.results = new LinkedList<>();
        List<Parameter> params = task.getParameters();
        int paramsCount = params.size();

        for (int rIdx = 0; rIdx < numReturns; rIdx++) {
            Parameter p = params.get(params.size() - numReturns + rIdx);
            this.results.add(generateLocalParameter(p));
        }
        paramsCount -= numReturns;
        if (hasTarget) {
            Parameter p = params.get(params.size() - numReturns - 1);
            this.target = generateLocalParameter(p);
            paramsCount--;
        }

        for (int paramIdx = 0; paramIdx < paramsCount; paramIdx++) {
            Parameter p = params.get(paramIdx);

            this.arguments.add(generateLocalParameter(p));
        }

        this.slaveWorkersNodeNames = slaveWorkersNodeNames;

        AbstractMethodImplementation absMethodImpl = (AbstractMethodImplementation) this.impl;
        this.reqs = absMethodImpl.getRequirements();
    }

    private LocalParameter generateLocalParameter(Parameter p) {
        if (p.getType() == DataType.COLLECTION_T) {
            CollectionParameter cp = (CollectionParameter) p;
            LocalParameterCollection lpc = new LocalParameterCollection(p);
            for (Parameter subParam : cp.getParameters()) {
                lpc.addParameter(generateLocalParameter(subParam));
            }
            return lpc;
        } else if (p.getType() == DataType.DICT_COLLECTION_T) {
            DictCollectionParameter cp = (DictCollectionParameter) p;
            LocalParameterDictCollection lpdc = new LocalParameterDictCollection(p);
            for (Map.Entry<Parameter, Parameter> entry : cp.getParameters().entrySet()) {
                lpdc.addParameter(generateLocalParameter(entry.getKey()), generateLocalParameter(entry.getValue()));
            }
            return lpdc;
        } else {
            return new LocalParameter(p);
        }
    }

    @Override
    public void submit() throws Exception {
        listener.allInputDataOnWorker(this);
        this.getResourceNode().runJob(this);
    }

    @Override
    public void cancelJob() throws Exception {
    }

    @Override
    public String getHostName() {
        return this.getResourceNode().getName();
    }

    @Override
    public TaskType getType() {
        return TaskType.METHOD;
    }

    @Override
    public TaskType getTaskType() {
        return taskType;
    }

    @Override
    public String toString() {
        return "LocalJob JobId" + this.jobId + " for task " + this.impl.getSignature();
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return (AbstractMethodImplementation) this.impl;
    }

    @Override
    public boolean isDebugEnabled() {
        return DEBUG;
    }

    @Override
    public List<LocalParameter> getParams() {
        return this.arguments;
    }

    @Override
    public LocalParameter getTarget() {
        return this.target;
    }

    @Override
    public List<LocalParameter> getResults() {
        return this.results;
    }

    @Override
    public ResourceDescription getRequirements() {
        return this.reqs;
    }

    @Override
    public List<String> getSlaveNodesNames() {
        return this.slaveWorkersNodeNames;
    }

    @Override
    public String getParallelismSource() {
        return this.taskParams.getParallelismSource();
    }

}
