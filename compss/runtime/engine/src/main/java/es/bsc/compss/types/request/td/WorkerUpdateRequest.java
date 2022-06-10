/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.request.td;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.updates.ResourceUpdate;
import es.bsc.compss.types.tracing.TraceEvent;


/**
 * The AddCloudNodeRequest represents a request to add a new resource ready to execute to the resource pool.
 */
public class WorkerUpdateRequest<T extends WorkerResourceDescription> extends TDRequest {

    private final Worker<T> worker;
    private final ResourceUpdate<T> ru;


    /**
     * Constructs a AddCloudNodeRequest with all its parameters.
     *
     * @param worker Worker that has been added.
     * @param update Resource update.
     */
    public WorkerUpdateRequest(Worker<T> worker, ResourceUpdate<T> update) {
        this.worker = worker;
        this.ru = update;
    }

    /**
     * Returns the associated worker to update.
     * 
     * @return The associated worker to update.
     */
    public Worker<T> getWorker() {
        return this.worker;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        ts.updateWorker(this.worker, this.ru);
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.WORKER_UPDATE_REQUEST;
    }

}
