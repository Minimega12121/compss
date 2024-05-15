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
package es.bsc.compss.types.data.accessid;

import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.DataVersion;


public interface EngineDataAccessId extends DataAccessId {

    DataInfo getAccessedDataInfo();

    void commit();

    void cancel(boolean keepModified);

    EngineDataAccessId consolidateValidVersions();

    String toDebugString();


    interface ReadingDataAccessId extends EngineDataAccessId, DataAccessId.ReadingDataAccessId {

        @Override
        EngineDataInstanceId getReadDataInstance();

        /**
         * Returns the data version read by the access.
         *
         * @return data version read
         */
        DataVersion getReadDataVersion();
    }

    interface WritingDataAccessId extends EngineDataAccessId, DataAccessId.WritingDataAccessId {

        @Override
        EngineDataInstanceId getWrittenDataInstance();

        /**
         * Returns the written data version.
         *
         * @return data version written
         */
        DataVersion getWrittenDataVersion();
    }
}
