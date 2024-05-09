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
package es.bsc.compss.types.data.info;

import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.params.DataOwner;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.util.ErrorManager;


public abstract class StandardDataInfo<T extends DataParams> extends DataInfo<T> {

    public StandardDataInfo(T data, DataOwner owner) {
        super(data, owner);
    }

    @Override
    public final EngineDataAccessId willAccess(AccessParams.AccessMode mode) {
        EngineDataAccessId daId = null;
        switch (mode) {
            case C:
            case R:
                this.willBeRead();
                daId = new RAccessId(this, this.currentVersion);
                break;

            case W:
                this.willBeWritten();
                daId = new WAccessId(this, this.currentVersion);
                break;

            case CV:
            case RW:
                this.willBeRead();
                DataVersion readInstance = this.currentVersion;
                this.willBeWritten();
                DataVersion writtenInstance = this.currentVersion;
                if (readInstance != null) {
                    daId = new RWAccessId(this, readInstance, writtenInstance);
                } else {
                    ErrorManager.warn("Previous instance for data" + this.dataId + " is null.");
                }
                break;
        }
        if (DEBUG && daId != null) {
            LOGGER.debug(daId.toDebugString());
        }
        return daId;
    }

    /**
     * Marks the data to be read.
     */
    private void willBeRead() {
        this.currentVersion.versionUsed();
        this.currentVersion.willBeRead();
    }

    /**
     * Marks the data to be written.
     */
    protected void willBeWritten() {
        this.currentVersionId++;
        DataVersion validPred = currentVersion;
        if (validPred.hasBeenCancelled()) {
            validPred = validPred.getPreviousValidPredecessor();
        }
        DataVersion newVersion = new DataVersion(this.dataId, this.currentVersionId, validPred);
        newVersion.willBeWritten();
        this.versions.put(this.currentVersionId, newVersion);
        this.currentVersion = newVersion;
        this.currentVersion.versionUsed();
    }
}
