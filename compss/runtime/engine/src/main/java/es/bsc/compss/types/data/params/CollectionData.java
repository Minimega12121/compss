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

import es.bsc.compss.types.data.info.CollectionInfo;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;


public class CollectionData extends DataParams {

    private final String collectionId;


    /**
     * Constructs a new DataParams for a collection.
     *
     * @param owner Owner of the collection
     * @param collectionId Id of the collection
     */
    public CollectionData(DataOwner owner, String collectionId) {
        super(owner);
        this.collectionId = collectionId;
    }

    @Override
    public String getDescription() {
        return "collection  " + this.collectionId;
    }

    @Override
    protected DataInfo registerData() {
        DataInfo cInfo = new CollectionInfo(this);
        DataOwner owner = this.getOwner();
        owner.registerCollectionData(this.collectionId, cInfo);
        return cInfo;
    }

    @Override
    public DataInfo getRegisteredData() {
        DataOwner owner = this.getOwner();
        return owner.getCollectionData(this.collectionId);
    }

    @Override
    protected DataInfo unregisterData() throws ValueUnawareRuntimeException {
        DataOwner owner = this.getOwner();
        return owner.removeCollectionData(this.collectionId);
    }

    public String getCollectionId() {
        return this.collectionId;
    }

    @Override
    public void deleteLocal() throws Exception {
        // No need to do anything to remove the local instance
    }
}
