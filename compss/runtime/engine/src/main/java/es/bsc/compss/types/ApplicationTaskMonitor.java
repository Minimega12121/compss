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
package es.bsc.compss.types;

public interface ApplicationTaskMonitor {

    /**
     * Notifies the monitor that a new task has been created.
     * 
     * @param t Created task
     */
    void onTaskCreation(Task t);

    /**
     * A task analysis has been started.
     * 
     * @param t task whose analysis started
     */
    void onTaskAnalysisStart(Task t);

    /**
     * A task analysis has finished.
     * 
     * @param t task whose analysis finished
     * @param hasEdge @return {@literal true}, if the task has requested an edge in the graph; {@literal false},
     *            otherwise.
     */
    void onTaskAnalysisEnd(Task t, boolean hasEdge);
}
