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

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.DataVersion;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Component to handle the specific data structures such as file names, versions, renamings and values.
 */
public class DataInfoProvider {

    // Component logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.DIP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();


    /**
     * New Data Info Provider instance.
     */
    public DataInfoProvider() {
        LOGGER.info("Initialization finished");
    }

    /**
     * DataAccess interface: registers a new data access.
     *
     * @param access Access Parameters.
     * @return The registered access Id.
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public EngineDataAccessId registerAccessToExistingData(AccessParams access) throws ValueUnawareRuntimeException {
        access.checkAccessValidity();
        return registerDataAccess(access);
    }

    /**
     * DataAccess interface: registers a new data access.
     *
     * @param access Access Parameters.
     * @return The registered access Id.
     */
    public EngineDataAccessId registerDataAccess(AccessParams access) {
        DataInfo dInfo = access.getDataInfo();
        if (dInfo == null) {
            if (DEBUG) {
                LOGGER.debug("FIRST access to " + access.getDataDescription());
            }
            dInfo = access.getData().createDataInfo();
            DataVersion dv = dInfo.getCurrentDataVersion();
            access.registerValueForVersion(dv);
        } else {
            if (DEBUG) {
                LOGGER.debug("Another access to " + access.getDataDescription());
            }
        }

        EngineDataAccessId daId = dInfo.willAccess(access.getMode());
        access.externalRegister();
        return daId;
    }

    /**
     * Marks the access from the main as finished.
     * 
     * @param access access being completed
     */
    public void finishDataAccess(AccessParams access, EngineDataInstanceId generatedData) {
        if (generatedData != null && access.resultRemainOnMain()) {
            generatedData.getVersion().valueOnMain();
        }
        DataInfo dInfo = access.getDataInfo();
        // First access to this file
        if (dInfo == null) {
            LOGGER.warn(access.getDataDescription() + " has not been accessed before");
            return;
        }
        EngineDataAccessId daid = dInfo.getLastAccess(access.getMode());
        if (daid == null) {
            LOGGER.warn(access.getDataDescription() + " has not been accessed before");
            return;
        }
        dataHasBeenAccessed(daid);
    }

    /**
     * Removes the versions associated with the given DataAccessId {@code dAccId} to if the task was canceled or not.
     *
     * @param dAccId DataAccessId.
     */
    public void dataAccessHasBeenCanceled(EngineDataAccessId dAccId, boolean keepModified) {
        Integer dataId = dAccId.getDataId();
        DataInfo di = DataInfo.get(dataId);
        if (di != null) {
            Integer rVersionId;
            Integer wVersionId;
            boolean deleted = false;
            switch (dAccId.getDirection()) {
                case C:
                case R:
                    rVersionId = ((RAccessId) dAccId).getReadDataInstance().getVersionId();
                    deleted = di.canceledReadVersion(rVersionId);
                    break;
                case CV:
                case RW:
                    rVersionId = ((RWAccessId) dAccId).getReadDataInstance().getVersionId();
                    wVersionId = ((RWAccessId) dAccId).getWrittenDataInstance().getVersionId();
                    if (keepModified) {
                        di.versionHasBeenRead(rVersionId);
                        // read data version can be removed
                        di.tryRemoveVersion(rVersionId);
                        deleted = di.versionHasBeenWritten(wVersionId);
                    } else {
                        di.canceledReadVersion(rVersionId);
                        deleted = di.canceledWriteVersion(wVersionId);
                    }
                    break;
                default:// case W:
                    wVersionId = ((WAccessId) dAccId).getWrittenDataInstance().getVersionId();
                    deleted = di.canceledWriteVersion(wVersionId);
                    break;
            }

            if (deleted) {
                di.deregister();
            }
        } else {
            LOGGER.debug("Access of Data" + dAccId.getDataId() + " in Mode " + dAccId.getDirection().name()
                + " can not be cancelled because do not exist in DIP.");
        }
    }

    /**
     * Marks that a given data {@code dAccId} has been accessed.
     *
     * @param dAccId DataAccessId.
     */
    public void dataHasBeenAccessed(EngineDataAccessId dAccId) {
        Integer dataId = dAccId.getDataId();
        DataInfo di = DataInfo.get(dataId);
        if (di != null) {
            Integer rVersionId = null;
            Integer wVersionId;
            boolean deleted = false;

            if (dAccId.isRead()) {
                rVersionId = ((ReadingDataAccessId) dAccId).getReadDataInstance().getVersionId();
                deleted = di.versionHasBeenRead(rVersionId);
            }

            if (dAccId.isWrite()) {
                wVersionId = ((WritingDataAccessId) dAccId).getWrittenDataInstance().getVersionId();
                if (rVersionId == null) {
                    rVersionId = wVersionId - 1;
                }
                di.tryRemoveVersion(rVersionId);
                deleted = di.versionHasBeenWritten(wVersionId);
            }

            if (deleted) {
                di.deregister();
            }
        } else {
            LOGGER.warn("Access of Data" + dAccId.getDataId() + " in Mode " + dAccId.getDirection().name()
                + "can not be mark as accessed because do not exist in DIP.");
        }
    }

    /**
     * Marks a data for deletion.
     *
     * @param data data to be deleted
     * @return DataInfo associated with the data to remove
     * @throws ValueUnawareRuntimeException the runtime is not aware of the data
     */
    public DataInfo deleteData(DataParams data) throws ValueUnawareRuntimeException {
        if (DEBUG) {
            LOGGER.debug("Deleting Data associated to " + data.getDescription());
        }

        DataInfo dataInfo = data.removeDataInfo();
        if (dataInfo == null) {
            if (DEBUG) {
                LOGGER.debug("No data found for data associated to " + data.getDescription());
            }
            throw new ValueUnawareRuntimeException();
        }
        // We delete the data associated with all the versions of the same object
        if (dataInfo.delete()) {
            dataInfo.deregister();
        }
        return dataInfo;
    }

}
