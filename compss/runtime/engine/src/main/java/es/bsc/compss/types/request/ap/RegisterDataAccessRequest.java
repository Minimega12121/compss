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
import es.bsc.compss.types.TaskListener;
import es.bsc.compss.types.accesses.DataAccessesInfo;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.access.MainAccess;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.tracing.TraceEvent;

import java.util.concurrent.Semaphore;


public class RegisterDataAccessRequest implements APRequest, TaskListener {

    private final MainAccess access;
    private EngineDataAccessId accessId;

    private int pendingOperation = 0;
    private boolean released = false;
    private ValueUnawareRuntimeException unawareException = null;
    private final Semaphore sem;


    /**
     * Creates a new request to register a data access.
     *
     * @param access description of the access done by the main
     */
    public RegisterDataAccessRequest(MainAccess access) {
        this.access = access;
        this.sem = new Semaphore(0);
    }

    /**
     * Returns the associated access parameters.
     *
     * @return The associated access parameters.
     */
    public MainAccess getAccess() {
        return this.access;
    }

    /**
     * Returns the waiting semaphore.
     *
     * @return The waiting semaphore.
     */
    public EngineDataAccessId getAccessId() {
        return this.accessId;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) {
        try {
            AccessParams accessParams = this.access.getParameters();
            if (DEBUG) {
                LOGGER.debug("Registering access " + accessParams.toString() + " from App "
                    + this.access.getApp().getId() + "'s main code");
            }
            accessParams.checkAccessValidity();
            this.accessId = accessParams.register();
            if (this.accessId == null) {
                if (DEBUG) {
                    LOGGER.debug("Accessing a canceled data from main code. Returning null");
                }
            } else {
                if (DEBUG) {
                    LOGGER.debug("Registered access to data " + this.accessId.getDataId() + " from main code");
                }

                if (this.accessId.isRead()) {
                    EngineDataAccessId.ReadingDataAccessId rdaId = (ReadingDataAccessId) this.accessId;
                    EngineDataInstanceId di = rdaId.getReadDataInstance();
                    Application app = this.access.getApp();
                    app.getCP().mainAccess(di);

                    int dataId = this.accessId.getDataId();
                    // Retrieve writers information
                    DataAccessesInfo dai = DataAccessesInfo.get(dataId);
                    if (dai != null) {
                        EngineDataInstanceId depInstance;
                        if (this.accessId.isWrite()) {
                            depInstance = ((WritingDataAccessId) this.accessId).getWrittenDataInstance();
                        } else {
                            depInstance = di;
                        }
                        dai.mainAccess(this, depInstance);
                    }
                }
            }

        } catch (ValueUnawareRuntimeException e) {
            this.unawareException = e;
        }
        if (pendingOperation == 0) {
            this.notifyReady();
        }
        sem.release();
    }

    /**
     * Waits for the value's producing tasks to complete releasing and recovering the resources if needed.
     *
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public void waitForCompletion() throws ValueUnawareRuntimeException {
        // Wait for request processing
        sem.acquireUninterruptibly();

        boolean stalled = false;
        Application app = this.access.getApp();
        synchronized (this) {
            LOGGER.info("App " + app.getId() + " waits for data to be produced");
            if (!released) {
                LOGGER.info("App " + app.getId() + " releases its resources.");
                stalled = true;
                app.stalled();
            }
        }
        LOGGER.info("App " + app.getId() + " awaits for data to be produced.");
        // Wait for producing task completion
        sem.acquireUninterruptibly();
        LOGGER.info("App " + app.getId() + " waiting data is produced.");
        // Wait for app to have resources
        if (stalled) {
            LOGGER.info("App " + app.getId() + " waits for resources.");
            app.readyToContinue(sem);
            sem.acquireUninterruptibly();
            LOGGER.info("App " + app.getId() + " resources are ready.");
        }
        if (this.unawareException != null) {
            throw this.unawareException;
        }
    }

    public void addPendingOperation() {
        pendingOperation++;
    }

    @Override
    public void taskFinished() {
        synchronized (this) {
            pendingOperation--;
            if (pendingOperation == 0) {
                notifyReady();
            }
        }
    }

    private void notifyReady() {
        if (accessId != null) {
            int dataId = this.accessId.getDataId();
            if (DEBUG) {
                LOGGER.debug("Data " + dataId + " available for main access");
            }
            this.accessId = this.accessId.consolidateValidVersions();
        } else {
            if (DEBUG) {
                LOGGER.debug("Inexisting data");
            }
        }
        released = true;
        sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.REGISTER_DATA_ACCESS;
    }

}
