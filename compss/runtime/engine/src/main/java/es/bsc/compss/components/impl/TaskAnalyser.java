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
import es.bsc.compss.types.TaskState;
import es.bsc.compss.types.accesses.DataAccessesInfo;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.parameter.impl.Parameter;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.util.ErrorManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Class to analyze the data dependencies between tasks.
 */
public class TaskAnalyser {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TA_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();


    /**
     * Creates a new Task Analyzer instance.
     */
    public TaskAnalyser() {
        LOGGER.info("Initialization finished");
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
}
