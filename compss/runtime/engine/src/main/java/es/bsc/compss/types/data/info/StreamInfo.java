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
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;
import es.bsc.compss.types.data.params.DataOwner;
import es.bsc.compss.types.data.params.StreamData;
import es.bsc.compss.util.ErrorManager;

import java.util.concurrent.Semaphore;


public class StreamInfo extends DataInfo<StreamData> {

    /**
     * Creates a new StreamInfo instance for the given stream.
     *
     * @param stream description of the stream related to the info
     * @param owner owner of the StreamInfo being created
     */
    public StreamInfo(StreamData stream, DataOwner owner) {
        super(stream, owner);
        int code = stream.getCode();
        owner.registerObjectData(code, this);
    }

    /**
     * Returns the object hashcode.
     *
     * @return The object hashcode.
     */
    public int getCode() {
        return this.getParams().getCode();
    }

    @Override
    public EngineDataAccessId willAccess(AccessMode mode) {
        EngineDataAccessId daId;
        switch (mode) {
            case R:
                this.currentVersion.versionUsed();
                this.currentVersion.willBeRead();
                daId = new RAccessId(this, this.currentVersion);
                break;

            case W:
                this.currentVersion.willBeWritten();
                this.currentVersion.versionUsed();
                daId = new WAccessId(this, this.currentVersion);
                break;
            default: // cases C, CV, RW
                ErrorManager.warn("Unsupported type of access (" + mode + ") for stream " + this.dataId);
                daId = null;
        }
        if (DEBUG && daId != null) {
            LOGGER.debug(daId.toDebugString());
        }
        return daId;
    }

    @Override
    public void waitForDataReadyToDelete(Semaphore sem) {
        // Nothing to wait for
    }

}
