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
package es.bsc.compss.components.impl;

import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.TaskState;
import es.bsc.compss.types.accesses.DataAccessesInfo;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.parameter.impl.CollectiveParameter;
import es.bsc.compss.types.parameter.impl.DependencyParameter;
import es.bsc.compss.types.parameter.impl.ObjectParameter;
import es.bsc.compss.types.parameter.impl.Parameter;
import es.bsc.compss.types.request.ap.BarrierRequest;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.util.ErrorManager;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StubItf;


/**
 * Class to analyze the data dependencies between tasks.
 */
public class TaskAnalyser {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TA_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();
    private static final String TASK_FAILED = "Task failed: ";
    private static final String TASK_CANCELED = "Task canceled: ";


    /**
     * Creates a new Task Analyzer instance.
     */
    public TaskAnalyser() {
        LOGGER.info("Initialization finished");
    }

    /**
     * Process the dependencies of a new task {@code currentTask}.
     *
     * @param currentTask Task.
     */
    public void processTask(Task currentTask) {
        TaskDescription description = currentTask.getTaskDescription();
        LOGGER.info("New " + description.getType().toString().toLowerCase() + " task: Name:" + description.getName()
            + "), ID = " + currentTask.getId() + " APP = " + currentTask.getApplication().getId());

        Application app = currentTask.getApplication();
        app.newTask(currentTask);
        app.getGH().startTaskAnalysis(currentTask);

        // Check scheduling enforcing data
        int constrainingParam = -1;

        // Process parameters
        boolean taskHasEdge = processTaskParameters(currentTask, constrainingParam);
        registerIntermediateParameter(currentTask);
        markIntermediateParametersToDelete(currentTask);
        app.getGH().endTaskAnalysis(currentTask, taskHasEdge);

        // Prepare checkpointer for task
        app.getCP().newTask(currentTask);
    }

    private boolean processTaskParameters(Task currentTask, int constrainingParam) {
        List<Parameter> parameters = currentTask.getParameters();
        boolean taskHasEdge = false;
        for (int paramIdx = 0; paramIdx < parameters.size(); paramIdx++) {
            boolean isConstraining = paramIdx == constrainingParam;
            Parameter param = parameters.get(paramIdx);
            boolean paramHasEdge = registerParameterAccessAndAddDependencies(currentTask, param, isConstraining);
            taskHasEdge = taskHasEdge || paramHasEdge;
        }
        return taskHasEdge;
    }

    private void markIntermediateParametersToDelete(Task task) {
        for (Parameter p : task.getParameterDataToRemove()) {
            if (p.isPotentialDependency()) {
                DependencyParameter dp = (DependencyParameter) p;
                try {
                    dp.getAccess().getData().delete();
                } catch (ValueUnawareRuntimeException e) {
                    // If not existing, the parameter was already removed. No need to do anything
                }
            }
        }
    }

    private void registerIntermediateParameter(Task task) {
        for (Parameter p : task.getIntermediateParameters()) {
            registerParameterAccessAndAddDependencies(task, p, false);
        }
    }

    /**
     * Registers a data access from the main code and notifies when the data is available.
     *
     * @param rdar request indicating the data being accessed
     * @return The registered access Id.
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public EngineDataAccessId processMainAccess(RegisterDataAccessRequest rdar) throws ValueUnawareRuntimeException {
        AccessParams access = rdar.getAccessParams();
        if (DEBUG) {
            LOGGER.debug("Registering access " + access.toString() + " from main code");
        }
        access.checkAccessValidity();
        EngineDataAccessId daId = access.register();
        if (daId == null) {
            if (DEBUG) {
                LOGGER.debug("Accessing a canceled data from main code. Returning null");
            }
            return null;
        }
        if (DEBUG) {
            LOGGER.debug("Registered access to data " + daId.getDataId() + " from main code");
        }

        if (daId.isRead()) {
            ReadingDataAccessId rdaId = (ReadingDataAccessId) daId;
            EngineDataInstanceId di = rdaId.getReadDataInstance();
            Application app = access.getApp();
            app.getCP().mainAccess(di);

            int dataId = daId.getDataId();
            // Retrieve writers information
            DataAccessesInfo dai = DataAccessesInfo.get(dataId);
            if (dai != null) {
                EngineDataInstanceId depInstance;
                if (daId.isWrite()) {
                    depInstance = ((WritingDataAccessId) daId).getWrittenDataInstance();
                } else {
                    depInstance = di;
                }
                dai.mainAccess(rdar, depInstance);
            }
        }
        return daId;
    }

    /**
     * Registers the end of execution of task @{code task}.
     *
     * @param aTask Ended task.
     * @param checkpointing {@literal true} if task has been recovered by the checkpoint management
     */
    public void endTask(AbstractTask aTask, boolean checkpointing) {
        int taskId = aTask.getId();
        long start = System.currentTimeMillis();
        if (aTask instanceof Task) {
            Task task = (Task) aTask;
            boolean isFree = task.isFree();
            TaskState taskState = task.getStatus();
            LOGGER.info("Notification received for task " + taskId + " with end status " + taskState);
            // Check status
            if (!isFree) {
                LOGGER.debug("Task " + taskId + " is not registered as free. Waiting for other executions to end");
                return;
            }

            switch (taskState) {
                case FAILED:
                    OnFailure onFailure = task.getOnFailure();
                    if (onFailure == OnFailure.RETRY || onFailure == OnFailure.FAIL) {
                        // Raise error
                        ErrorManager.error(TASK_FAILED + task);
                        return;
                    }
                    if (onFailure == OnFailure.IGNORE || onFailure == OnFailure.CANCEL_SUCCESSORS) {
                        // Show warning
                        ErrorManager.warn(TASK_FAILED + task);
                    }
                    break;
                case CANCELED:
                    // Show warning
                    ErrorManager.warn(TASK_CANCELED + task);
                    break;
                default:
                    // Do nothing
            }

            // Mark parameter accesses
            if (DEBUG) {
                LOGGER.debug("Marking accessed parameters for task " + taskId);
            }

            for (Parameter param : task.getTaskDescription().getParameters()) {
                updateParameter(task, param);
            }

            // When a task can have internal temporal parameters,
            // the not used ones have to be updated to perform the data delete
            for (Parameter param : task.getUnusedIntermediateParameters()) {
                updateParameter(task, param);
            }

            // Free barrier dependencies
            if (DEBUG) {
                LOGGER.debug("Freeing barriers for task " + taskId);
            }

            // Free dependencies
            // Free task data dependencies
            if (DEBUG) {
                LOGGER.debug("Releasing waiting tasks for task " + taskId);
            }
            task.notifyListeners();

            // Check if the finished task was the last writer of a file, but only if task generation has finished
            // Task generation is finished if we are on noMoreTasks but we are not on a barrier
            if (DEBUG) {
                LOGGER.debug("Checking result file transfers for task " + taskId);
            }

            Application app = task.getApplication();
            // Release task groups of the task
            app.endTask(task);

            TaskMonitor registeredMonitor = task.getTaskMonitor();
            switch (taskState) {
                case FAILED:
                    registeredMonitor.onFailure();
                    break;
                case CANCELED:
                    registeredMonitor.onCancellation();
                    break;
                default:
                    registeredMonitor.onCompletion();
            }

            // Releases commutative groups dependent and releases all the waiting tasks
            task.releaseCommutativeGroups();

            // If we are not retrieving the checkpoint
            if (!checkpointing) {
                if (DEBUG) {
                    LOGGER.debug("Checkpoint saving task " + taskId);
                }
                app.getCP().endTask(task);
            }
        }

        // Release data dependent tasks
        if (DEBUG) {
            LOGGER.debug("Releasing data dependant tasks for task " + taskId);
        }
        aTask.releaseDataDependents();

        if (DEBUG) {
            long time = System.currentTimeMillis() - start;
            LOGGER.debug("Task " + taskId + " end message processed in " + time + " ms.");
        }
    }

    /**
     * Deletes the specified data and its renamings.
     *
     * @param data data to be deleted
     * @param applicationDelete whether the user code requested to delete the data ({@literal true}) or was removed by
     *            the runtime ({@literal false})
     * @throws ValueUnawareRuntimeException the runtime is not aware of the data
     */
    public void deleteData(DataParams data, boolean applicationDelete) throws ValueUnawareRuntimeException {
        DataInfo dataInfo = data.delete();
        int dataId = dataInfo.getDataId();
        LOGGER.info("Deleting data " + dataId);

        // Deleting checkpointed data that is obsolete, INOUT that has a newest version
        if (applicationDelete) {
            Application app = data.getApp();
            app.getCP().deletedData(dataInfo);
        }

        DataAccessesInfo dai = DataAccessesInfo.remove(dataId);
        if (dai != null) {
            switch (dai.getDataType()) {
                case STREAM_T:
                case EXTERNAL_STREAM_T:
                    // No data to delete
                    break;
                case FILE_T:
                    // Remove file data form the list of written files
                    Application app = data.getApp();
                    FileInfo fInfo = (FileInfo) data.getRegisteredData();
                    app.removeWrittenFile(fInfo);
                    break;
                default:
                    // Nothing to do for other types
                    break;
            }
        } else {
            LOGGER.warn("Writters info for data " + dataId + " not found.");
        }
    }

    /*
     * *************************************************************************************************************
     * DATA DEPENDENCY MANAGEMENT PRIVATE METHODS
     ***************************************************************************************************************/
    private boolean registerParameterAccessAndAddDependencies(Task currentTask, Parameter p, boolean isConstraining) {
        boolean hasParamEdge = false;
        if (p.isCollective()) {
            CollectiveParameter cp = (CollectiveParameter) p;
            Application app = currentTask.getApplication();
            app.getGH().startGroupingEdges();
            for (Parameter content : cp.getElements()) {
                boolean hasCollectionParamEdge =
                    registerParameterAccessAndAddDependencies(currentTask, content, isConstraining);
                hasParamEdge = hasParamEdge || hasCollectionParamEdge;
            }
            app.getGH().stopGroupingEdges();
        } else {
            if (p.getType() == DataType.OBJECT_T) {
                ObjectParameter op = (ObjectParameter) p;
                // Check if its PSCO class and persisted to infer its type
                if (op.getValue() instanceof StubItf && ((StubItf) op.getValue()).getID() != null) {
                    op.setType(DataType.PSCO_T);
                }
            }
        }

        // Inform the Data Manager about the new accesses
        EngineDataAccessId daId;
        AccessParams access = p.getAccess();
        if (access != null) {
            daId = access.register();
        } else {
            daId = null;
        }

        if (p.isCollective()) {
            try {
                deleteData(access.getData(), false);
            } catch (ValueUnawareRuntimeException e) {
                // If not existing, the collection was already removed. No need to do anything
            }
        }

        if (daId != null) {
            // Add parameter dependencies
            DependencyParameter dp = (DependencyParameter) p;
            dp.setDataAccessId(daId);
            hasParamEdge = addDependencies(currentTask, isConstraining, dp);
        } else {
            // Basic types do not produce access dependencies
            currentTask.registerFreeParam(p);
        }
        // Return data Id
        return hasParamEdge;
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

    private void updateParameter(Task task, Parameter p) {
        if (p.isCollective()) {
            CollectiveParameter cParam = (CollectiveParameter) p;
            for (Parameter sp : cParam.getElements()) {
                updateParameter(task, sp);
            }
        }
        if (p.isPotentialDependency()) {
            DependencyParameter dp = (DependencyParameter) p;
            EngineDataAccessId dAccId = dp.getDataAccessId();
            if (dAccId == null) {
                LOGGER.warn("Parameter for task " + task.getId()
                    + " has no access ID. It could be from a cancelled type. Ignoring ... ");
                return;
            }
            int dataId = dAccId.getDataId();

            DataType type = p.getType();
            if (type != DataType.DIRECTORY_T || type != DataType.STREAM_T || type != DataType.EXTERNAL_STREAM_T) {
                if (DEBUG) {
                    int currentTaskId = task.getId();
                    LOGGER.debug("Removing writters info for datum " + dataId + " and task " + currentTaskId);
                }
                DataAccessesInfo dai = DataAccessesInfo.get(dataId);
                if (dai != null) {
                    switch (dp.getDirection()) {
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
                LOGGER.debug("Treating that data " + dAccId + " has been accessed at " + dp.getDataTarget());
            }

            if ((task.getOnFailure() == OnFailure.CANCEL_SUCCESSORS && (task.getStatus() == TaskState.FAILED))
                || (task.getStatus() == TaskState.CANCELED && task.getOnFailure() != OnFailure.IGNORE)) {
                DataInfo.cancelAccess(dAccId, task.wasSubmited());
            } else {
                DataInfo.commitAccess(dAccId);
            }
        }
    }
}
