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
package es.bsc.compss.types.data.accessparams;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.DataVersion;
import es.bsc.compss.types.data.params.DataOwner;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;

import java.io.Serializable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Description of the access parameters to an object, file, stream, or binding-object.
 */
public abstract class AccessParams<D extends DataParams> implements Serializable {

    public static enum AccessMode {

        R(true, false), // Read
        W(false, true), // Write
        RW(true, true), // ReadWrite
        C(true, false), // Concurrent
        CV(true, true) // Commutative
        ;


        private final boolean read;
        private final boolean write;


        AccessMode(boolean read, boolean write) {
            this.read = read;
            this.write = write;
        }

        public final boolean isRead() {
            return this.read;
        }

        public boolean isWrite() {
            return write;
        }
    }


    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // Component logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.DIP_COMP);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    protected final Application app;
    protected final D data;
    protected final AccessMode mode;


    private static AccessMode getAccessMode(Direction d) {
        AccessMode am = AccessMode.R;
        switch (d) {
            case IN:
            case IN_DELETE:
                am = AccessParams.AccessMode.R;
                break;
            case OUT:
                am = AccessParams.AccessMode.W;
                break;
            case INOUT:
                am = AccessParams.AccessMode.RW;
                break;
            case CONCURRENT:
                am = AccessParams.AccessMode.C;
                break;
            case COMMUTATIVE:
                am = AccessParams.AccessMode.CV;
                break;
        }
        return am;
    }

    /**
     * Creates a new AccessParams instance.
     *
     * @param app Application accessing the data
     * @param data Data being accessed
     * @param dir operation performed.
     */
    protected AccessParams(Application app, D data, Direction dir) {
        this.app = app;
        this.data = data;
        this.mode = getAccessMode(dir);
    }

    /**
     * Returns the application accessing the value.
     * 
     * @return application accessing the value.
     */
    public Application getApp() {
        return app;
    }

    /**
     * Returns the data being accessed.
     * 
     * @return data being accessed
     */
    public D getData() {
        return data;
    }

    /**
     * Returns the access mode.
     *
     * @return The access mode.
     */
    public final AccessMode getMode() {
        return this.mode;
    }

    public DataInfo getDataInfo() {
        return data.getRegisteredData(this.app);
    }

    public final String getDataDescription() {
        return data.getDescription();
    }

    /**
     * Verifies that the runtime is aware of the value and the access should be registered.
     *
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public abstract void checkAccessValidity() throws ValueUnawareRuntimeException;

    /**
     * Registers a new data access.
     *
     * @return The registered access Id.
     */
    public final EngineDataAccessId register() {
        DataInfo dInfo = this.data.getRegisteredData(this.app);
        if (dInfo == null) {
            if (DEBUG) {
                LOGGER.debug("FIRST access to " + this.getDataDescription());
            }

            dInfo = this.data.register(this.app);
            DataVersion dv = dInfo.getCurrentDataVersion();
            this.registerValueForVersion(dv);
        } else {
            if (DEBUG) {
                LOGGER.debug("Another access to " + this.getDataDescription());
            }
        }

        EngineDataAccessId daId = dInfo.willAccess(this.mode);
        this.externalRegister();
        return daId;
    }

    protected abstract void registerValueForVersion(DataVersion dv);

    /**
     * Registers the access into an external service.
     */
    protected abstract void externalRegister();

    /**
     * Marks the access from the main as finished.
     *
     * @param generatedData data resulting from the access
     */
    public void finish(EngineDataInstanceId generatedData) {
        if (generatedData != null && this.resultRemainOnMain()) {
            generatedData.getVersion().valueOnMain();
        }
        DataInfo dInfo = this.data.getRegisteredData(this.app);
        // First access to this file
        if (dInfo == null) {
            LOGGER.warn(this.getDataDescription() + " has not been accessed before");
            return;
        }
        EngineDataAccessId daid = dInfo.getLastAccess(this.mode);
        if (daid == null) {
            LOGGER.warn(this.getDataDescription() + " has not been accessed before");
            return;
        }
        daid.commit();
    }

    /**
     * Returns whether the result of the access should be marked as remaining on the Main process memory.
     * 
     * @return {@literal true} if the result is to be marked; {@literal false} otherwise.
     */
    public abstract boolean resultRemainOnMain();

}
