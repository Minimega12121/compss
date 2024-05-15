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
package es.bsc.compss.types.data.access;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Handling of an access from the main code to a data.
 */
public abstract class MainAccess<V extends Object, D extends DataParams, P extends AccessParams<D>> {

    // Component logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.API);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    private final Application app;
    private final P parameters;


    protected MainAccess(Application app, P params) {
        this.app = app;
        this.parameters = params;
    }

    /**
     * Returns the application performing the access.
     *
     * @return application performing the access.
     */
    public Application getApp() {
        return app;
    }

    /**
     * Returns the access parameters associated to the Access.
     *
     * @return parameters of the access
     */
    public final P getParameters() {
        return parameters;
    }

    /**
     * Returns the value expected to be returned when there is no available version for the data.
     *
     * @return Returns the value expected to be returned when there is no available version for the data.
     */
    public abstract V getUnavailableValueResponse();

    /**
     * Fetches the last version of the accessed data.
     *
     * @param daId Data Access Id.
     * @return last version of the accessed data.
     */
    public abstract V fetch(EngineDataAccessId daId);

    /**
     * Registers the main access and detects the dependencies.
     *
     * @param rdar element to notify when dependencies are discovered
     * @return The registered access Id.
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public EngineDataAccessId register(RegisterDataAccessRequest rdar) throws ValueUnawareRuntimeException {
        AccessParams accessParams = this.parameters;
        if (DEBUG) {
            Long appId = this.getApp().getId();
            LOGGER.debug("Registering access " + accessParams.toString() + " from App " + appId + "'s main code");
        }
        accessParams.checkAccessValidity();
        EngineDataAccessId accessId = accessParams.register();
        if (accessId == null) {
            if (DEBUG) {
                LOGGER.debug("Accessing a canceled data from main code. Returning null");
            }
        } else {
            if (DEBUG) {
                LOGGER.debug("Registered access to data " + accessId.getDataId() + " from main code");
            }

            if (accessId.isRead()) {
                EngineDataAccessId.ReadingDataAccessId rdaId = (EngineDataAccessId.ReadingDataAccessId) accessId;
                EngineDataInstanceId di = rdaId.getReadDataInstance();
                Application app = this.getApp();
                app.getCP().mainAccess(di);

                // Retrieve writers information
                DataInfo dai = accessId.getAccessedDataInfo();
                if (dai != null) {
                    dai.mainAccess(rdar, accessId);
                }
            }
        }
        return accessId;
    }

    /**
     * Returns whether the registration of the access leads to its immediate finalization.
     *
     * @return {@literal true} if the finalization of the access is to be registers; {@literal false} otherwise.
     */
    public abstract boolean isAccessFinishedOnRegistration();

    protected static DataLocation createLocalLocation(SimpleURI targetURI) {
        DataLocation targetLocation = null;
        try {
            targetLocation = DataLocation.createLocation(Comm.getAppHost(), targetURI);
        } catch (IOException ioe) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetURI, ioe);
        }
        return targetLocation;
    }

}
