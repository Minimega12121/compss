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

import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataVersion;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.request.exceptions.NonExistingValueException;
import es.bsc.compss.util.ErrorManager;

import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


// Information about a datum and its versions
public abstract class DataInfo<T extends DataParams> {

    // CONSTANTS
    private static final int FIRST_FILE_ID = 1;
    private static final int FIRST_VERSION_ID = 1;

    // Component logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.DIP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();
    
    // Map: file identifier -> file information
    private static final Map<Integer, DataInfo> ID_TO_DATA = new TreeMap<>();

    protected static int nextDataId = FIRST_FILE_ID;

    // Data identifier
    protected final int dataId;
    // Generating application
    protected final T params;

    // Current version
    protected DataVersion currentVersion;
    // Data and version identifier management
    protected int currentVersionId;

    // Versions of the datum
    // Map: version identifier -> version
    protected TreeMap<Integer, DataVersion> versions;
    // private boolean toDelete;

    protected int deletionBlocks;
    protected final LinkedList<DataVersion> pendingDeletions;
    protected final LinkedList<Integer> canceledVersions;

    protected boolean deleted;


    /**
     * Creates a new DataInfo instance with and registers a new LogicalData.
     *
     * @param data description of the data related to the info
     */
    public DataInfo(T data) {
        this.dataId = nextDataId++;
        this.params = data;
        this.versions = new TreeMap<>();
        this.currentVersionId = FIRST_VERSION_ID;
        this.currentVersion = new DataVersion(dataId, 1, null);
        this.versions.put(currentVersionId, currentVersion);
        this.deletionBlocks = 0;
        this.pendingDeletions = new LinkedList<>();
        this.canceledVersions = new LinkedList<>();
        this.deleted = false;
        ID_TO_DATA.put(dataId, this);
    }

    /**
     * Returns the data Id.
     *
     * @return The data Id.
     */
    public final int getDataId() {
        return this.dataId;
    }

    /**
     * Returns the description of the data related to the info.
     *
     * @return description of the data related to the info
     */
    public T getParams() {
        return params;
    }

    /**
     * Returns the application generating the DataInfo.
     *
     * @return the application generating the DataInfo.
     */
    public Application getGeneratingAppId() {
        return this.params.getApp();
    }

    /**
     * Returns the current version Id.
     *
     * @return The current version Id.
     */
    public final int getCurrentVersionId() {
        return this.currentVersionId;
    }

    /**
     * Returns the current data version.
     *
     * @return The current data version.
     */
    public final DataVersion getCurrentDataVersion() {
        return this.currentVersion;
    }

    /**
     * Reconstruct the last access to the data as if were of a given mode.
     *
     * @param mode mode being access
     */
    public DataAccessId getLastAccess(AccessMode mode) {
        // Version management
        DataAccessId daId = null;
        if (this.currentVersion != null) {
            switch (mode) {
                case C:
                case R:
                    daId = new RAccessId(this.currentVersion);
                    break;
                case W:
                    daId = new WAccessId(this.currentVersion);
                    break;
                case CV:
                case RW:
                    DataVersion readInstance = this.versions.get(this.currentVersionId - 1);
                    if (readInstance != null) {
                        daId = new RWAccessId(readInstance, this.currentVersion);
                    } else {
                        LOGGER.warn("Previous instance for data" + this.dataId + " is null.");
                    }
                    break;
            }
        } else {
            LOGGER.warn("Current instance for data" + this.dataId + " is null.");
        }
        return daId;
    }

    /**
     * Registers a new access to the data.
     *
     * @param mode access mode of the operation performed on the data
     * @return description of the access performed
     */
    public DataAccessId willAccess(AccessMode mode) {
        DataAccessId daId = null;
        switch (mode) {
            case C:
            case R:
                this.willBeRead();
                daId = new RAccessId(this.currentVersion);
                if (DEBUG) {
                    StringBuilder sb = new StringBuilder("");
                    sb.append("Access:").append("\n");
                    sb.append("  * Type: R").append("\n");
                    sb.append("  * Read Datum: d").append(daId.getDataId()).append("v")
                        .append(((RAccessId) daId).getRVersionId()).append("\n");
                    LOGGER.debug(sb.toString());
                }
                break;

            case W:
                this.willBeWritten();
                daId = new WAccessId(this.currentVersion);
                if (DEBUG) {
                    StringBuilder sb = new StringBuilder("");
                    sb.append("Access:").append("\n");
                    sb.append("  * Type: W").append("\n");
                    sb.append("  * Write Datum: d").append(daId.getDataId()).append("v")
                        .append(((WAccessId) daId).getWVersionId()).append("\n");
                    LOGGER.debug(sb.toString());
                }
                break;

            case CV:
            case RW:
                this.willBeRead();
                DataVersion readInstance = this.currentVersion;
                this.willBeWritten();
                DataVersion writtenInstance = this.currentVersion;
                if (readInstance != null) {
                    daId = new RWAccessId(readInstance, writtenInstance);
                    if (DEBUG) {
                        StringBuilder sb = new StringBuilder("");
                        sb.append("Access:").append("\n");
                        sb.append("  * Type: RW").append("\n");
                        sb.append("  * Read Datum: d").append(daId.getDataId()).append("v")
                            .append(((RWAccessId) daId).getRVersionId()).append("\n");
                        sb.append("  * Write Datum: d").append(daId.getDataId()).append("v")
                            .append(((RWAccessId) daId).getWVersionId()).append("\n");
                        LOGGER.debug(sb.toString());
                    }
                } else {
                    ErrorManager.warn("Previous instance for data" + this.dataId + " is null.");
                }
                break;
        }
        return daId;
    }

    /**
     * Marks the data to be read.
     */
    private final void willBeRead() {
        this.currentVersion.versionUsed();
        this.currentVersion.willBeRead();
    }

    /**
     * Returns whether the data is expected to be read or not.
     *
     * @return {@code true} if there are pending reads to the data, {@code false} otherwise.
     */
    public final boolean isToBeRead() {
        return this.currentVersion.hasPendingLectures();
    }

    /**
     * Returns whether the data has been cancelled or not.
     *
     * @return {@code true} if the data has been cancelled, {@code false} otherwise.
     */
    public final boolean hasBeenCanceled() {
        return this.currentVersion.hasBeenUsed();
    }

    /**
     * Returns whether the specified version {@code versionId} has been read or not.
     *
     * @param versionId Version Id.
     * @return {@code true} if the version Id has no pending reads, {@code false} otherwise.
     */
    public final boolean versionHasBeenRead(int versionId) {
        DataVersion readVersion = this.versions.get(versionId);
        if (readVersion.hasBeenRead()) {
            Comm.removeData(readVersion.getDataInstanceId().getRenaming(), true);
            this.versions.remove(versionId);
            return this.versions.isEmpty();
        }
        return false;
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

    /**
     * Returns whether the data has already been written or not.
     *
     * @param versionId Version Id.
     * @return {@code true} if the data has been written, {@code false} otherwise.
     */
    public final boolean versionHasBeenWritten(int versionId) {
        DataVersion writtenVersion = versions.get(versionId);
        if (writtenVersion.hasBeenWritten()) {
            Comm.removeData(writtenVersion.getDataInstanceId().getRenaming(), true);
            this.versions.remove(versionId);
            // return (this.toDelete && versions.size() == 0);
            return this.versions.isEmpty();
        }
        return false;
    }

    /**
     * Increases the number of deletion blocks.
     */
    public final void blockDeletions() {
        this.deletionBlocks++;
    }

    /**
     * Decreases the number of deletion blocks and returns whether all the pending deletions are completed or not.
     *
     * @return {@code true} if all the pending deletions have been removed, {@code false} otherwise.
     */
    public final boolean unblockDeletions() {
        this.deletionBlocks--;
        if (this.deletionBlocks == 0) {
            for (DataVersion version : this.pendingDeletions) {
                if (version.markToDelete()) {
                    Comm.removeData(version.getDataInstanceId().getRenaming(), true);
                    this.versions.remove(version.getDataInstanceId().getVersionId());
                }
            }
            if (this.versions.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Delete DataInfo (can be overwritten by implementations).
     */
    public boolean delete() {
        this.deleted = true;
        if (this.deletionBlocks > 0) {
            this.pendingDeletions.addAll(this.versions.values());
        } else {
            LinkedList<Integer> removedVersions = new LinkedList<>();
            for (DataVersion version : this.versions.values()) {
                String sourceName = version.getDataInstanceId().getRenaming();
                if (version.markToDelete()) {
                    Comm.removeData(sourceName, true);
                    removedVersions.add(version.getDataInstanceId().getVersionId());
                }
            }
            for (int versionId : removedVersions) {
                this.versions.remove(versionId);
            }
            if (this.versions.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Waits for the data to be ready to be deleted.
     *
     * @param sem Semaphore.
     * @throws NonExistingValueException the data to delete does not actually exist
     */
    public abstract void waitForDataReadyToDelete(Semaphore sem) throws NonExistingValueException;

    /**
     * Returns whether the current version is marked to deleted or not.
     *
     * @return {@code true} if the current version must be deleted, {@code false} otherwise.
     */
    public final boolean isCurrentVersionToDelete() {
        return this.currentVersion.isToDelete();
    }

    /**
     * Returns the first data version.
     *
     * @return The first data version.
     */
    public final DataVersion getFirstVersion() {
        return versions.get(1);
    }

    /**
     * Tries to remove the given version {@code versionId}.
     *
     * @param versionId Version Id.
     */
    public final void tryRemoveVersion(Integer versionId) {
        DataVersion readVersion = this.versions.get(versionId);

        if (readVersion != null && readVersion.markToDelete()) {
            Comm.removeData(readVersion.getDataInstanceId().getRenaming(), true);
            this.versions.remove(versionId);
        }

    }

    /**
     * Cancels the given read version {@code versionId}.
     *
     * @param versionId Version Id.
     * @return {@literal true} if there are no more versions for the data; {@literal false} otherwise.
     */
    public final boolean canceledReadVersion(Integer versionId) {
        DataVersion readVersion = this.versions.get(versionId);
        if (!deleted && readVersion.isToDelete() && readVersion.hasBeenUsed()) {
            readVersion.unmarkToDelete();
        }
        if (readVersion.hasBeenRead()) {
            Comm.removeData(readVersion.getDataInstanceId().getRenaming(), true);
            this.versions.remove(versionId);
            // return (this.toDelete && versions.size() == 0);
            return this.versions.isEmpty();
        }
        return false;

    }

    /**
     * Cancels the given version {@code versionId}.
     *
     * @param versionId Version Id.
     * @return true if no more versions
     */
    public final boolean canceledWriteVersion(Integer versionId) {
        DataVersion version = this.versions.get(versionId);
        version.versionCancelled();
        this.canceledVersions.add(versionId);
        if (versionId == currentVersionId) {
            Integer lastVersion = this.currentVersionId;
            while (this.canceledVersions.contains(lastVersion)) {
                tryRemoveVersion(lastVersion);
                lastVersion = lastVersion - 1;
            }
            if (lastVersion > 1) {
                this.currentVersionId = lastVersion;
                this.currentVersion = this.versions.get(this.currentVersionId);
                return false;
            } else if (lastVersion == 1) {
                DataVersion firstVersion = this.getFirstVersion();
                if (firstVersion != null && firstVersion.hasBeenUsed()) {
                    this.currentVersionId = lastVersion;
                    this.currentVersion = firstVersion;
                    return false;
                } else {
                    return true;
                }
            } else {
                return true;
            }
        }
        return false;
    }

    /**
     * Deregisters the data from the collection.
     */
    public void deregister() {
        ID_TO_DATA.remove(this.dataId);
    }

    /**
     * Obtains the DataInfo for a given dataId.
     * 
     * @param dataId Id of the data to retrieve
     * @return DataInfo for the dataId passed in as parameter
     */
    public static DataInfo get(Integer dataId) {
        return ID_TO_DATA.get(dataId);
    }
}