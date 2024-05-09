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
package es.bsc.compss.types.data.params;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class DataParams {

    // Component logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.DIP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private final DataOwner owner;


    public DataParams(DataOwner owner) {
        this.owner = owner;
    }

    /**
     * Returns the owner of the data.
     * 
     * @return owner of the data
     */
    public DataOwner getOwner() {
        return owner;
    }

    /**
     * Returns a string describing the data.
     * 
     * @return data description.
     */
    public abstract String getDescription();

    /**
     * Registers the data in the access dependency system.
     * 
     * @return DataInfo associated with the data to remove
     */
    public final DataInfo register() {
        if (DEBUG) {
            LOGGER.debug("Registering Data associated to " + this.getDescription());
        }
        return registerData();
    }

    /**
     * Marks a data for deletion.
     *
     * @return DataInfo associated with the data to remove
     * @throws ValueUnawareRuntimeException the runtime is not aware of the data
     */
    public final DataInfo delete() throws ValueUnawareRuntimeException {
        if (DEBUG) {
            LOGGER.debug("Deleting Data associated to " + this.getDescription());
        }
        try {
            return this.unregisterData();
        } catch (ValueUnawareRuntimeException vure) {
            if (DEBUG) {
                LOGGER.debug("No data found for data associated to " + this.getDescription());
            }
            throw vure;
        }
    }

    protected abstract DataInfo registerData();

    public abstract DataInfo getRegisteredData();

    protected abstract DataInfo unregisterData() throws ValueUnawareRuntimeException;

    /**
     * Deletes the local instance of the data.
     * 
     * @throws Exception An error arised during the deletion
     */
    public abstract void deleteLocal() throws Exception;

}
