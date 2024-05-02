/*
 *  Copyright 2002-2023 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.parameter.impl;

import es.bsc.compss.api.ParameterMonitor;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskState;
import es.bsc.compss.types.accesses.DataAccessesInfo;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.parameter.StdIOStream;

import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import storage.StubItf;


public abstract class DependencyParameter<T extends AccessParams> extends Parameter
    implements es.bsc.compss.types.parameter.DependencyParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // Loggers
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TA_COMP);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    private final T access;

    private EngineDataAccessId daId;
    private Object dataSource;
    private String dataTarget; // URI (including PROTOCOL) where to find the data within the executing resource


    /**
     * Creates a new DependencyParameter instance from the given parameters.
     *
     * @param type Parameter type.
     * @param direction Parameter direction.
     * @param access description of the access performed on the data
     * @param stream Parameter IO stream mode.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param weight Parameter weight.
     * @param keepRename Parameter keep rename property.
     * @param monitor object to notify to changes on the parameter
     */
    protected DependencyParameter(T access, DataType type, Direction direction, StdIOStream stream, String prefix,
        String name, String contentType, double weight, boolean keepRename, ParameterMonitor monitor) {
        super(type, direction, stream, prefix, name, contentType, weight, keepRename, monitor);
        this.access = access;
    }

    @Override
    public boolean isPotentialDependency() {
        return true;
    }

    @Override
    public final T getAccess() {
        return access;
    }

    @Override
    public EngineDataAccessId getDataAccessId() {
        return this.daId;
    }

    /**
     * Sets a new data access id.
     *
     * @param daId New data access id.
     */
    public void setDataAccessId(EngineDataAccessId daId) {
        this.daId = daId;
    }

    @Override
    public String getOriginalName() {
        return NO_NAME;
    }

    @Override
    public Object getDataSource() {
        return this.dataSource;
    }

    @Override
    public void setDataSource(Object dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public String getDataTarget() {
        return this.dataTarget;
    }

    @Override
    public void setDataTarget(String target) {
        this.dataTarget = target;
    }

    @Override
    public String generateDataTargetName(String tgtName) {
        if (getType().equals(DataType.PSCO_T) || getType().equals(DataType.EXTERNAL_PSCO_T)) {
            return getDataTarget();
        } else {
            return tgtName;
        }
    }

    @Override
    public String toString() {
        return "DependencyParameter";
    }

    @Override
    public boolean isSourcePreserved() {
        return this.daId.isPreserveSourceData();
    }

    @Override
    public boolean isTargetFlexible() {
        return true;
    }

    @Override
    public boolean register(Task task, boolean isConstraining) {
        boolean hasParamEdge = false;

        // Inform the Data Manager about the new accesses
        EngineDataAccessId daId;
        AccessParams access = this.getAccess();
        daId = access.register();

        // Add parameter dependencies
        this.setDataAccessId(daId);
        hasParamEdge = addDependencies(task, isConstraining, this);

        // Return data Id
        return hasParamEdge;
    }

    @Override
    public void cancel(Task task) {
        EngineDataAccessId dAccId = this.getDataAccessId();
        updateParameter(task);
        DataInfo.cancelAccess(dAccId, task.wasSubmited());
    }

    @Override
    public void commit(Task task) {
        EngineDataAccessId dAccId = this.getDataAccessId();
        updateParameter(task);
        DataInfo.commitAccess(dAccId);
    }

    @Override
    public void remove() {
        try {
            this.getAccess().getData().delete();
        } catch (ValueUnawareRuntimeException e) {
            // If not existing, the parameter was already removed. No need to do anything
        }
    }

    private boolean addDependencies(Task currentTask, boolean isConstraining, DependencyParameter dp) {
        // Add dependencies to the graph and register output values for future dependencies
        boolean hasParamEdge = false;
        EngineDataAccessId daId = dp.getDataAccessId();
        int dataId = daId.getDataId();
        DataAccessesInfo dai = DataAccessesInfo.get(dataId);
        switch (dp.getAccess().getMode()) {
            case R:
                hasParamEdge = checkInputDependency(currentTask, dp, false, dataId, dai, isConstraining);
                break;
            case RW:
                hasParamEdge = checkInputDependency(currentTask, dp, false, dataId, dai, isConstraining);
                registerOutputValues(currentTask, dp, false, dai);
                break;
            case W:
                // Register output values
                registerOutputValues(currentTask, dp, false, dai);
                break;
            case C:
                hasParamEdge = checkInputDependency(currentTask, dp, true, dataId, dai, isConstraining);
                registerOutputValues(currentTask, dp, true, dai);
                break;
            case CV:
                hasParamEdge = checkInputDependency(currentTask, dp, false, dataId, dai, isConstraining);
                registerOutputValues(currentTask, dp, false, dai);
                break;
        }
        return hasParamEdge;
    }

    private boolean checkInputDependency(Task currentTask, DependencyParameter dp, boolean isConcurrent, int dataId,
        DataAccessesInfo dai, boolean isConstraining) {
        if (DEBUG) {
            LOGGER.debug("Checking READ dependency for datum " + dataId + " and task " + currentTask.getId());
        }
        boolean hasEdge = false;
        if (dai != null) {
            hasEdge = dai.readValue(currentTask, dp, isConcurrent);
            if (isConstraining) {
                AbstractTask lastWriter = dai.getConstrainingProducer();
                currentTask.setEnforcingTask((Task) lastWriter);
            }
        } else {
            // Task is free
            if (DEBUG) {
                LOGGER.debug("There is no last writer for datum " + dataId);
            }
            currentTask.registerFreeParam(dp);
        }
        return hasEdge;
    }

    /**
     * Registers the output values of the task {@code currentTask}.
     *
     * @param currentTask Task.
     * @param dp Dependency Parameter.
     * @param isConcurrent data access was done in concurrent mode
     * @param dai AccessInfo related to the data being accessed
     */
    private void registerOutputValues(Task currentTask, DependencyParameter dp, boolean isConcurrent,
        DataAccessesInfo dai) {
        int currentTaskId = currentTask.getId();
        int dataId = dp.getDataAccessId().getDataId();
        Application app = currentTask.getApplication();

        if (DEBUG) {
            LOGGER.debug("Checking WRITE dependency for datum " + dataId + " and task " + currentTaskId);
        }

        if (dai == null) {
            dai = DataAccessesInfo.createAccessInfo(dataId, dp.getType());
        }
        dai.writeValue(currentTask, dp, isConcurrent);

        // Update file and PSCO lists
        switch (dp.getType()) {
            case DIRECTORY_T:
            case FILE_T:
                FileInfo fInfo = (FileInfo) dp.getAccess().getDataInfo();
                app.addWrittenFile(fInfo);
                break;
            default:
                // Nothing to do with basic types
                // Objects are not checked, their version will be only get if the main accesses them
                break;
        }

        if (DEBUG) {
            LOGGER.debug("New writer for datum " + dataId + " is task " + currentTaskId);
        }
    }

    private void updateParameter(Task task) {
        EngineDataAccessId dAccId = this.getDataAccessId();
        if (dAccId == null) {
            LOGGER.warn("Parameter for task " + task.getId()
                + " has no access ID. It could be from a cancelled type. Ignoring ... ");
            return;
        }
        int dataId = dAccId.getDataId();

        DataType type = this.getType();
        if (type != DataType.DIRECTORY_T || type != DataType.STREAM_T || type != DataType.EXTERNAL_STREAM_T) {
            if (DEBUG) {
                int currentTaskId = task.getId();
                LOGGER.debug("Removing writters info for datum " + dataId + " and task " + currentTaskId);
            }
            DataAccessesInfo dai = DataAccessesInfo.get(dataId);
            if (dai != null) {
                switch (this.getDirection()) {
                    case OUT:
                    case INOUT:
                        dai.completedProducer(task);
                        break;
                    default:
                        break;
                }
            }
        }

        if (DEBUG) {
            LOGGER.debug("Treating that data " + dAccId + " has been accessed at " + this.getDataTarget());
        }
    }
}
