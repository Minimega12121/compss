/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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

package es.bsc.compss.util;

import es.bsc.cepbatools.extrae.Wrapper;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.tracing.TraceScript;
import es.bsc.compss.util.types.ThreadTranslator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class Tracer {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String ERROR_TRACE_DIR = "ERROR: Cannot create trace directory";
    private static final String ERROR_MASTER_PACKAGE_FILEPATH =
        "Cannot locate master tracing package " + "on working directory";

    // Tracing script and file paths
    private static final String MASTER_TRACE_FILE = "master_compss_trace.tar.gz";
    protected static final String TRACE_PATH = File.separator + "trace" + File.separator;
    protected static final String TRACE_OUT_RELATIVE_PATH = TRACE_PATH + "tracer.out";
    protected static final String TRACE_ERR_RELATIVE_PATH = TRACE_PATH + "tracer.err";
    public static final String TRACE_SUBDIR = "trace";
    public static final String TO_MERGE_SUBDIR = "to_merge";

    // Naming
    public static final String MASTER_TRACE_SUFFIX = "_compss";
    public static final String TRACE_ROW_FILE_EXTENTION = ".row";
    public static final String TRACE_PRV_FILE_EXTENTION = ".prv";
    public static final String TRACE_PCF_FILE_EXTENTION = ".pcf";

    // Extrae loaded properties
    private static final boolean IS_CUSTOM_EXTRAE_FILE =
        (System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE) != null)
            && !System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE).isEmpty()
            && !System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE).equals("null");
    private static final String EXTRAE_FILE =
        IS_CUSTOM_EXTRAE_FILE ? System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE) : "null";

    // Extrae environment flags
    public static final String LD_PRELOAD = "LD_PRELOAD";
    public static final String EXTRAE_CONFIG_FILE = "EXTRAE_CONFIG_FILE";
    public static final String EXTRAE_USE_POSIX_CLOCK = "EXTRAE_USE_POSIX_CLOCK";

    public static final int EVENT_END = 0;

    // Tracing modes
    public static final int ADVANCED_MODE = 2;
    public static final int BASIC_MODE = 1;
    public static final int DISABLED = 0;
    public static final int SCOREP_MODE = -1;
    public static final int MAP_MODE = -2;
    protected static int tracingLevel = DISABLED;

    private static String installDir = System.getenv(COMPSsConstants.COMPSS_HOME);
    protected static boolean tracingTaskDependencies;
    private static String traceDirPath;
    private static Map<String, TraceHost> hostToSlots;
    private static AtomicInteger hostId;

    public static boolean tracerAlreadyLoaded = false;

    private static int numPthreadsEnabled = 0;

    // Hashmap of the predecessors
    private static HashMap<Integer, ArrayList<Integer>> predecessorsMap;


    /**
     * Initializes tracer creating the trace folder. If extrae's tracing is used (level > 0) then the current node
     * (master) sets its nodeID (taskID in extrae) to 0, and its number of tasks to 1 (a single program).
     *
     * @param logDirPath Path to the log directory
     * @param level type of tracing: -3: arm-ddt, -2: arm-map, -1: scorep, 0: off, 1: extrae-basic, 2: extrae-advanced
     */
    public static void init(String logDirPath, int level, boolean tracingTasks) {
        if (tracerAlreadyLoaded) {
            if (DEBUG) {
                LOGGER.debug("Tracing already initialized " + level + "no need for a second initialization");
            }
            return;
        }
        tracerAlreadyLoaded = true;
        if (DEBUG) {
            LOGGER.debug("Initializing tracing with level " + level);
            LOGGER.debug("Tracing task dependencies: " + tracingTasks);
        }

        hostId = new AtomicInteger(1);
        hostToSlots = new HashMap<>();
        predecessorsMap = new HashMap<>();
        tracingTaskDependencies = tracingTasks;

        if (!logDirPath.endsWith(File.separator)) {
            logDirPath += logDirPath;
        }
        traceDirPath = logDirPath + "trace" + File.separator;
        if (!new File(traceDirPath).mkdir()) {
            ErrorManager.error(ERROR_TRACE_DIR);
        }

        tracingLevel = level;

        if (Tracer.extraeEnabled()) {
            setUpWrapper(0, 1);
        } else {
            if (DEBUG) {
                if (Tracer.scorepEnabled()) {
                    LOGGER.debug("Initializing scorep.");
                } else {
                    if (Tracer.mapEnabled()) {
                        LOGGER.debug("Initializing arm-map.");
                    }
                }
            }
        }
    }

    /**
     * Initialized the Extrae wrapper.
     *
     * @param taskId taskId of the node
     * @param numTasks num of tasks for that node
     */
    protected static void setUpWrapper(int taskId, int numTasks) {
        synchronized (Tracer.class) {
            if (DEBUG) {
                LOGGER.debug("Initializing extrae Wrapper.");
            }
            Wrapper.SetTaskID(taskId);
            Wrapper.SetNumTasks(numTasks);
        }
    }

    /**
     * Returns if the current execution is being instrumented by extrae.
     *
     * @return true if currently instrumented by extrae
     */
    public static boolean extraeEnabled() {
        return tracingLevel > 0;
    }

    /**
     * Returns if the current execution is being instrumented by scorep.
     *
     * @return true if currently instrumented by scorep
     */
    public static boolean scorepEnabled() {
        return tracingLevel == Tracer.SCOREP_MODE;
    }

    /**
     * Returns if the current execution is being instrumented by arm-map.
     *
     * @return true if currently instrumented by arm-map
     */
    public static boolean mapEnabled() {
        return tracingLevel == Tracer.MAP_MODE;
    }

    /**
     * Returns if any kind of tracing is activated including ddt, map, scorep, or extrae).
     *
     * @return true if any kind of tracing is activated
     */
    public static boolean isActivated() {
        return tracingLevel != 0;
    }

    /**
     * Returns whether extrae is working and is activated in basic mode.
     *
     * @return true if extrae is enabled in basic mode
     */
    public static boolean basicModeEnabled() {
        return tracingLevel == Tracer.BASIC_MODE;
    }

    /**
     * Returns with which tracing level the Tracer has been initialized (0 if it's not active).
     *
     * @return int with tracing level (in [-3, -2, -1, 0, 1, 2])
     */
    public static int getLevel() {
        return tracingLevel;
    }

    /**
     * Returns true if task dependencies tracing is activated.
     *
     * @return true or false
     */
    public static boolean isTracingTaskDependencies() {

        return tracingTaskDependencies;
    }

    /**
     * Returns the config file used for extrae.
     *
     * @return path of extrae config file
     */
    public static String getExtraeFile() {
        return EXTRAE_FILE;
    }

    /**
     * When using extrae's tracing, this call enables the instrumentation of ALL created threads from here onwards until
     * the same number (n) of disablePThreads is called.
     */
    public static void enablePThreads(int n) {
        synchronized (Tracer.class) {
            numPthreadsEnabled += n;
            if (numPthreadsEnabled > 0) {
                Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS);
            }
        }
    }

    /**
     * When using extrae's tracing, when n reaches the number of enablePThreads, this call disables the instrumentation
     * of any created threads from here onwards. To reactivate it use enablePThreads()
     */
    public static void disablePThreads(int n) {
        synchronized (Tracer.class) {
            numPthreadsEnabled -= n;
            if (numPthreadsEnabled < 1) {
                numPthreadsEnabled = 0;
                Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS & ~Wrapper.EXTRAE_PTHREAD_OPTION);
            }
        }
    }

    /**
     * Adds a host name and its number of slots to a hashmap required to later merge the traces from each host into a
     * single one.
     *
     * @param name of the host
     * @param slots number of threads the host is expected to have (used in GAT, in NIO is 0, because they will be
     *            computed automatically
     * @return the next ID to be used during the initialization of the tracing in the given host.
     */
    public static int registerHost(String name, int slots) {
        if (DEBUG) {
            LOGGER.debug("Tracing: Registering host " + name + " in the tracing system");
        }
        int id;
        synchronized (hostToSlots) {
            if (hostToSlots.containsKey(name)) {
                if (DEBUG) {
                    LOGGER.debug("Host " + name + " already in tracing system, skipping");
                }
                return -1;
            }
            id = hostId.getAndIncrement();
            hostToSlots.put(name, new TraceHost(slots));
        }
        return id;
    }

    /**
     * Returns the next slot ID (thread) that will run a task (GAT only).
     *
     * @param host that is going to execute a task
     * @return the next thread ID available to execute task (don't care about real order)
     */
    public static int getNextSlot(String host) {
        int slot = hostToSlots.get(host).getNextSlot();
        if (DEBUG) {
            LOGGER.debug("Tracing: Getting slot " + slot + " of host " + host);
        }
        return slot;
    }

    /**
     * Signals that a slot ID (thread) of a host is free again.
     *
     * @param host that is going to have a slot freed
     * @param slot to be freed
     */
    public static void freeSlot(String host, int slot) {
        if (DEBUG) {
            LOGGER.debug("Tracing: Freeing slot " + slot + " of host " + host);
        }
        hostToSlots.get(host).freeSlot(slot);
    }

    public static int getRuntimeEventsType() {
        return TraceEventType.RUNTIME.code;
    }

    public static int getSyncType() {
        return TraceEventType.SYNC.code;
    }

    public static int getTaskTransfersType() {
        return TraceEventType.TASK_TRANSFERS.code;
    }

    public static int getDataTransfersType() {
        return TraceEventType.DATA_TRANSFERS.code;
    }

    public static int getTaskEventsType() {
        return TraceEventType.TASKS_FUNC.code;
    }

    public static int getTaskSchedulingType() {
        return TraceEventType.TASKS_ID.code;
    }

    public static int getInsideTasksEventsType() {
        return TraceEventType.BINDING_INSIDE_TASKS.code;
    }

    public static int getTasksCPUAffinityEventsType() {
        return TraceEventType.TASKS_CPU_AFFINITY.code;
    }

    public static int getTasksGPUAffinityEventsType() {
        return TraceEventType.TASKS_GPU_AFFINITY.code;
    }

    public static int getInsideTasksCPUAffinityEventsType() {
        return TraceEventType.BINDING_INSIDE_TASKS_CPU_AFFINITY.code;
    }

    public static int getInsideTasksGPUAffinityEventsType() {
        return TraceEventType.BINDING_INSIDE_TASKS_GPU_AFFINITY.code;
    }

    public static int getBindingInsideWorkerEventsType() {
        return TraceEventType.BINDING_INSIDE_WORKER.code;
    }

    public static int getBindingMasterEventsType() {
        return TraceEventType.BINDING_MASTER.code;
    }

    public static int getTaskTypeEventsType() {
        return TraceEventType.TASKTYPE.code;
    }

    public static int getCPUCountEventsType() {
        return TraceEventType.CPU_COUNTS.code;
    }

    public static int getGPUCountEventsType() {
        return TraceEventType.GPU_COUNTS.code;
    }

    public static int getReadyCountEventsType() {
        return TraceEventType.READY_COUNTS.code;
    }

    public static int getMemoryEventsType() {
        return TraceEventType.MEMORY.code;
    }

    public static int getDiskBWEventsType() {
        return TraceEventType.DISK_BW.code;
    }

    public static int getThreadIdEventsType() {
        return TraceEventType.THREAD_IDENTIFICATION.code;
    }

    public static TraceEvent getAcessProcessorRequestEvent(String eventType) {
        return TraceEvent.valueOf(eventType);
    }

    public static ArrayList<Integer> getPredecessors(int taskId) {
        return predecessorsMap.get(taskId);
    }

    public static void removePredecessor(int taskId) {
        predecessorsMap.remove(taskId);
    }

    /**
     * Adds id predecessors to list of predecessors.
     *
     * @param taskId Id of task
     * @param predecessorTaskId Id of predecessor task
     */
    public static void addPredecessors(int taskId, int predecessorTaskId) {
        ArrayList<Integer> predecessors = predecessorsMap.get(taskId);
        if (predecessors == null) {
            predecessors = new ArrayList<>();
        }
        predecessors.add(predecessorTaskId);
        predecessorsMap.put(taskId, predecessors);
    }

    /**
     * Returns the corresponding event ID for a TD request event type.
     *
     * @param eventType of the TD
     * @return the tracing event ID associated with eventType
     */
    public static TraceEvent getTaskDispatcherRequestEvent(String eventType) {
        TraceEvent event = null;
        try {
            event = TraceEvent.valueOf(eventType);
        } catch (Exception e) {
            LOGGER.error("Task Dispatcher event " + eventType + " is not present in Tracer's list ");
        }
        return event;
    }

    /**
     * Emits an event using extrae's Wrapper. Requires that Tracer has been initialized with lvl >0
     *
     * @param event event being emitted
     */
    public static void emitEvent(TraceEvent event) {
        synchronized (Tracer.class) {
            Wrapper.Event(event.getId(), event.getType());
        }

        if (DEBUG) {
            LOGGER.debug("Emitting synchronized event [type, id] = [" + event.getId() + " , " + event.getType() + "]");
        }
    }

    /**
     * Emits an event using extrae's Wrapper. Requires that Tracer has been initialized with lvl >0
     *
     * @param eventID ID of the event
     * @param eventType type of the event.
     */
    public static void emitEvent(long eventID, int eventType) {
        synchronized (Tracer.class) {
            Wrapper.Event(eventType, eventID);
        }

        if (DEBUG) {
            LOGGER.debug("Emitting synchronized event [type, id] = [" + eventType + " , " + eventID + "]");
        }
    }

    /**
     * Emits an event and the current PAPI counters activated using extrae's Wrapper. Requires that Tracer has been
     * initialized with lvl >0.
     *
     * @param taskId ID of the event
     * @param eventType type of the event.
     */
    public static void emitEventAndCounters(int taskId, int eventType) {
        synchronized (Tracer.class) {
            Wrapper.Eventandcounters(eventType, taskId);
        }

        if (DEBUG) {
            LOGGER.debug(
                "Emitting synchronized event with HW counters [type, taskId] = [" + eventType + " , " + taskId + "]");
        }
    }

    /**
     * Emits the end of an event using extrae's Wrapper. Requires that Tracer has been initialized with lvl >0
     *
     * @param event event being emitted
     */
    public static void emitEventEnd(TraceEvent event) {
        synchronized (Tracer.class) {
            Wrapper.Event(EVENT_END, event.getType());
        }

        if (DEBUG) {
            LOGGER.debug("Emitting synchronized event [type, id] = [" + EVENT_END + " , " + event.getType() + "]");
        }
    }

    /**
     * Emits a new communication event.
     *
     * @param send Whether it is a send event or not.
     * @param ownID Transfer own Id.
     * @param partnerID Transfer partner Id.
     * @param tag Transfer tag.
     * @param size Transfer size.
     */
    public static void emitCommEvent(boolean send, int ownID, int partnerID, int tag, long size) {
        synchronized (Tracer.class) {
            Wrapper.Comm(send, tag, (int) size, partnerID, ownID);
        }

        if (DEBUG) {
            LOGGER.debug("Emitting communication event [" + (send ? "SEND" : "REC") + "] " + tag + ", " + size + ", "
                + partnerID + ", " + ownID + "]");
        }
    }

    /**
     * End the extrae tracing system. Finishes master's tracing, generates both master and worker's packages, merges the
     * packages, and clean the intermediate traces.
     *
     * @param runtimeEvents label-Id pairs for the runtimeEvents
     */
    public static void fini(Map<String, Integer> runtimeEvents) {
        if (DEBUG) {
            LOGGER.debug("Tracing: finalizing");
        }

        synchronized (Tracer.class) {
            if (extraeEnabled()) {
                defineEvents(runtimeEvents);

                Tracer.stopWrapper();

                generateMasterPackage();
                transferMasterPackage();
                generateTrace();
                if (basicModeEnabled()) {
                    sortTrace();
                }
                cleanMasterPackage();
            } else {
                if (scorepEnabled()) {
                    // No master ScoreP trace - only Python Workers
                    generateTrace();
                }
            }
        }
    }

    /**
     * Updates the threads in .prv and .row classifying them in runtime or non runtime and assigning the corresponding
     * labels
     */
    private static void sortTrace() {
        String disable = System.getProperty(COMPSsConstants.DISABLE_CUSTOM_THREADS_TRACING);
        if (disable != null) {
            LOGGER.debug("Custom thread translation disabled");
            return;
        }
        LOGGER.debug("Tracing: Updating thread labels");
        File[] rowFileArray;
        File[] prvFileArray;
        try {
            String appLogDir = System.getProperty(COMPSsConstants.APP_LOG_DIR);
            File dir = new File(appLogDir + TRACE_SUBDIR);
            final String traceNamePrefix = Tracer.getTraceNamePrefix();
            rowFileArray = dir.listFiles((File d, String name) -> name.endsWith(TRACE_ROW_FILE_EXTENTION));
            prvFileArray = dir.listFiles((File d, String name) -> name.endsWith(TRACE_PRV_FILE_EXTENTION));
        } catch (Exception e) {
            ErrorManager.error(ERROR_MASTER_PACKAGE_FILEPATH, e);
            return;
        }
        try {
            if (rowFileArray != null && rowFileArray.length > 0) {
                File rowFile = rowFileArray[0];
                File prvFile = prvFileArray[0];
                ThreadTranslator thTranslator = new ThreadTranslator(prvFile);
                thTranslator.translatePrvFile(prvFile);
                thTranslator.translateRowFile(rowFile);
            }
        } catch (Exception e) {
            LOGGER.debug(e);
            LOGGER.debug(e.toString());
            ErrorManager.error("Could not update thread labels " + traceDirPath, e);
            e.printStackTrace();
        }
    }

    /**
     * Stops the extrae wrapper.
     */
    protected static void stopWrapper() {
        synchronized (Tracer.class) {
            LOGGER.debug("[Tracer] Disabling pthreads");
            Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS & ~Wrapper.EXTRAE_PTHREAD_OPTION);
            Wrapper.Fini();
            // End wrapper
            if (DEBUG) {
                LOGGER.debug("[Tracer] Finishing extrae");
            }
            Wrapper.SetOptions(Wrapper.EXTRAE_DISABLE_ALL_OPTIONS);
        }
    }

    /**
     * Iterates over all the tracing events and sets them in the Wrapper to generate the config. for the tracefile.
     *
     * @param runtimeEvents label-Id pairs for the runtimeEvents
     */
    private static void defineEvents(Map<String, Integer> runtimeEvents) {
        if (DEBUG) {
            LOGGER.debug("SignatureToId size: " + runtimeEvents.size());
        }

        for (TraceEventType type : TraceEventType.values()) {
            switch (type) {
                case TASKS_FUNC:
                    defineEventsForFunctions(type, runtimeEvents);
                    break;
                case BINDING_TASKS_FUNC:
                    // defineEventsForFunctions(type, runtimeEvents);
                    break;
                case TASKTYPE:
                    defineEventsForTaskType(type, MethodType.values());
                    break;
                default:
                    defineEventsForType(type);
            }
        }
    }

    private static void defineEventsForTaskType(TraceEventType type, MethodType[] types) {
        int size = types.length + 1;
        long[] values = new long[size];
        String[] descriptionValues = new String[size];
        values[0] = 0;
        descriptionValues[0] = "End";
        int i = 1;
        for (MethodType tp : types) {
            values[i] = tp.ordinal() + 1L;
            descriptionValues[i] = tp.name();
            ++i;
        }
        Wrapper.defineEventType(type.code, type.desc, values, descriptionValues);

    }

    private static void defineEventsForFunctions(TraceEventType type, Map<String, Integer> runtimeEvents) {
        int size = runtimeEvents.entrySet().size() + 1;
        long[] values = new long[size];
        String[] descriptionValues = new String[size];
        values[0] = 0;
        descriptionValues[0] = "End";
        int i = 1;
        for (Entry<String, Integer> entry : runtimeEvents.entrySet()) {
            String signature = entry.getKey();
            Integer methodId = entry.getValue();
            values[i] = methodId + 1L;
            LOGGER.debug("Tracing debug: " + signature);
            String methodName = signature.substring(signature.indexOf('.') + 1, signature.length());
            String mN = methodName.replace("(", "([").replace(")", "])");
            if (mN.contains(".")) {
                int start = mN.lastIndexOf(".");
                mN = "[" + mN.substring(0, start) + ".]" + mN.substring(start + 1);
            }
            descriptionValues[i] = mN;
            if (DEBUG) {
                LOGGER.debug("Tracing Funtion Event [i,methodId]: [" + i + "," + methodId + "] => value: " + values[i]
                    + ", Desc: " + descriptionValues[i]);
            }
            i++;
        }

        Wrapper.defineEventType(type.code, type.desc, values, descriptionValues);
    }

    private static void defineEventsForType(TraceEventType type) {
        boolean endable = type.endable;
        List<TraceEvent> events = TraceEvent.getByType(type);

        long[] values;
        String[] descriptions;
        int size = events.size();
        int offset = 0;
        if (endable) {
            values = new long[size + 1];
            values[0] = 0;
            descriptions = new String[size + 1];
            descriptions[0] = "End";
            offset = 1;
        } else {
            values = new long[size];
            descriptions = new String[size];
        }
        for (TraceEvent event : events) {
            values[offset] = event.getId();
            descriptions[offset] = event.getSignature();
            if (DEBUG) {
                LOGGER.debug("Tracing[API]: Type " + type.code + " Event " + offset + "=> value: " + values[offset]
                    + ", Desc: " + descriptions[offset]);
            }
            offset++;
        }
        Wrapper.defineEventType(type.code, type.desc, values, descriptions);
    }

    /**
     * Generate the tracing package for the master.
     */
    private static void generateMasterPackage() {
        if (DEBUG) {
            LOGGER.debug("Tracing: generating master package");
        }
        generatePackage(installDir, ".", "master", "0");
    }

    protected static void generatePackage(String installDir, String workingDir, String nodeName, String hostId) {
        try {
            int exitCode = 0;
            switch (tracingLevel) {
                case ADVANCED_MODE:
                case BASIC_MODE:
                    exitCode = TraceScript.package_extrae(installDir, workingDir, nodeName, hostId);
                    break;
                default: // DISABLED, SCOREP and ARM-MAP
                    // Do nothing
            }
            if (exitCode != 0) {
                ErrorManager.warn("Error generating " + nodeName + " package, exit code " + exitCode);
            }
        } catch (IOException e) {
            ErrorManager.warn("Error generating " + nodeName + " package", e);

        } catch (InterruptedException e) {
            ErrorManager.warn("Error generating " + nodeName + " package (interruptedException)", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Copy the tracing master package from the working directory. Node packages are transferred on NIOTracer of
     * GATTracer.
     */
    private static void transferMasterPackage() {
        if (DEBUG) {
            LOGGER.debug("Tracing: Transferring master package");
        }

        String filename = ProtocolType.FILE_URI.getSchema() + MASTER_TRACE_FILE;
        String filePath = "";
        try {
            SimpleURI uri = new SimpleURI(filename);
            filePath = new File(uri.getPath()).getCanonicalPath();
        } catch (Exception e) {
            ErrorManager.error(ERROR_MASTER_PACKAGE_FILEPATH, e);
            return;
        }

        try {
            Path source = Paths.get(filePath);
            Path target = Paths.get(traceDirPath + MASTER_TRACE_FILE);
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ioe) {
            ErrorManager.error("Could not copy the master trace package into " + traceDirPath, ioe);
        }
    }

    private static void generateTrace() {
        if (DEBUG) {
            LOGGER.debug("Tracing: Generating trace");
        }

        String traceName = "";
        String appName = System.getProperty(COMPSsConstants.APP_NAME);
        String label = System.getProperty(COMPSsConstants.TRACE_LABEL);

        if (appName != null && !appName.isEmpty() && !appName.equals("None")) {
            if (label != null && !label.isEmpty() && !label.equals("None")) {
                traceName = appName.concat("_" + label);
            } else {
                traceName = appName;
            }
        } else {
            if (label != null && !label.isEmpty() && !label.equals("None")) {
                traceName = label;
            }
        }

        int exitCode = 0;
        try {
            switch (tracingLevel) {
                case ADVANCED_MODE:
                case BASIC_MODE:
                    exitCode = TraceScript.gentrace_extrae(installDir, System.getProperty(COMPSsConstants.APP_LOG_DIR),
                        traceName, String.valueOf(hostToSlots.size() + 1));
                    break;
                case SCOREP_MODE:
                    exitCode = TraceScript.gentrace_scorep(installDir, System.getProperty(COMPSsConstants.APP_LOG_DIR),
                        traceName, String.valueOf(hostToSlots.size() + 1));
                    break;
                default: // DISABLEDand ARM-MAP
                    // Do nothing
            }
            if (exitCode != 0) {
                ErrorManager.warn("Error generating trace, exit code " + exitCode);
                return;
            }
        } catch (IOException e) {
            ErrorManager.warn("Error generating trace", e);
            return;
        } catch (InterruptedException e) {
            ErrorManager.warn("Error generating trace (interruptedException)", e);
            return;
        }

        String lang = System.getProperty(COMPSsConstants.LANG);
        if (lang.equalsIgnoreCase(COMPSsConstants.Lang.PYTHON.name()) && extraeEnabled()) {
            try {
                String appLogDir = System.getProperty(COMPSsConstants.APP_LOG_DIR);
                PythonTraceMerger t = new PythonTraceMerger(appLogDir);
                t.merge();
            } catch (Exception e) {
                ErrorManager.warn("Error while trying to merge files", e);
            }
        }
    }

    /**
     * Returns the beginning of the name of the trace files.
     */
    public static String getTraceNamePrefix() {
        String traceName = System.getProperty(COMPSsConstants.APP_NAME);
        String label = System.getProperty(COMPSsConstants.TRACE_LABEL);
        if (label != null && !label.isEmpty() && !label.equals("None")) {
            traceName = traceName.concat("_" + label);
        }
        return traceName + Tracer.MASTER_TRACE_SUFFIX;
    }

    /**
     * Removing the tracing temporal packages.
     */
    private static void cleanMasterPackage() {

        String filename = ProtocolType.FILE_URI.getSchema() + MASTER_TRACE_FILE;
        String filePath = "";
        try {
            SimpleURI uri = new SimpleURI(filename);
            filePath = new File(uri.getPath()).getCanonicalPath();
        } catch (Exception e) {
            ErrorManager.error(ERROR_MASTER_PACKAGE_FILEPATH, e);
            return;
        }

        if (DEBUG) {
            LOGGER.debug("Tracing: Removing tracing master package: " + filePath);
        }

        File f;
        try {
            f = new File(filePath);
            boolean deleted = f.delete();
            if (!deleted) {
                ErrorManager.warn("Unable to remove tracing temporary files of master node.");
            } else {
                if (DEBUG) {
                    LOGGER.debug("Deleted master tracing package.");
                }
            }
        } catch (Exception e) {
            ErrorManager.warn("Exception while trying to remove tracing temporary files of master node.", e);
        }
    }


    private static class TraceHost {

        private boolean[] slots;
        private int numFreeSlots;
        private int nextSlot;


        private TraceHost(int nslots) {
            this.slots = new boolean[nslots];
            this.numFreeSlots = nslots;
            this.nextSlot = 0;
        }

        private int getNextSlot() {
            if (numFreeSlots-- > 0) {
                while (slots[nextSlot]) {
                    nextSlot = (nextSlot + 1) % slots.length;
                }
                slots[nextSlot] = true;
                return nextSlot;
            } else {
                return -1;
            }
        }

        private void freeSlot(int slot) {
            slots[slot] = false;
            nextSlot = slot;
            numFreeSlots++;
        }
    }

}
