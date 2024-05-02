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
package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.accesses.DataAccessesInfo;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.tracing.TraceEvent;
import java.util.concurrent.Semaphore;


public class DeleteDataRequest extends APRequest {

    private final DataParams data;
    private final Semaphore sem;

    private ValueUnawareRuntimeException unawareException = null;
    private final boolean applicationDelete;


    /**
     * Creates a new request to delete a file.
     * 
     * @param data data to delete
     * @param applicationDelete Whether the deletion was requested by the user code of the application {@literal true},
     *            or automatically removed by the runtime {@literal false}.
     */
    public DeleteDataRequest(DataParams data, boolean applicationDelete) {
        this.data = data;
        this.sem = new Semaphore(0);
        this.applicationDelete = applicationDelete;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) {
        try {
            // File is involved in some task execution
            // File Won't be read by any future task or from the main code.
            // Remove it from the dependency analysis and the files to be transferred back
            LOGGER.info("[DeleteDataRequest] Deleting Data in Task Analyser");
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

        } catch (ValueUnawareRuntimeException vure) {
            unawareException = vure;
        }
        this.sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.DELETE_DATA;
    }

}
