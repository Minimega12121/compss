/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.executor.external.piped;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.executor.external.ExecutionPlatformMirror;
import es.bsc.compss.executor.external.ExternalExecutorException;
import es.bsc.compss.executor.external.commands.ExternalCommand.CommandType;
import es.bsc.compss.executor.external.piped.commands.AddExecutorPipeCommand;
import es.bsc.compss.executor.external.piped.commands.AddedExecutorPipeCommand;
import es.bsc.compss.executor.external.piped.commands.CancelJobCommand;
import es.bsc.compss.executor.external.piped.commands.ChannelCreatedPipeCommand;
import es.bsc.compss.executor.external.piped.commands.CreateChannelPipeCommand;
import es.bsc.compss.executor.external.piped.commands.ExecutorPIDQueryPipeCommand;
import es.bsc.compss.executor.external.piped.commands.ExecutorPIDReplyPipeCommand;
import es.bsc.compss.executor.external.piped.commands.PingPipeCommand;
import es.bsc.compss.executor.external.piped.commands.PipeCommand;
import es.bsc.compss.executor.external.piped.commands.PongPipeCommand;
import es.bsc.compss.executor.external.piped.commands.QuitPipeCommand;
import es.bsc.compss.executor.external.piped.commands.RemoveExecutorPipeCommand;
import es.bsc.compss.executor.external.piped.commands.RemovedExecutorPipeCommand;
import es.bsc.compss.executor.external.piped.commands.StartWorkerPipeCommand;
import es.bsc.compss.executor.external.piped.commands.WorkerStartedPipeCommand;
import es.bsc.compss.executor.external.piped.exceptions.ClosedPipeException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.StreamGobbler;
import es.bsc.compss.util.Tracer;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class PipedMirror implements ExecutionPlatformMirror<PipePair> {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);

    // Logger messages
    private static final String ERROR_PB_START = "Error starting ProcessBuilder";
    private static final String ERROR_W_START = "Error starting Worker";
    private static final String ERROR_W_PIPE = "Error on Worker pipe";

    protected static final String TOKEN_NEW_LINE = "\n";
    protected static final String TOKEN_SEP = " ";

    // Piper paths
    protected static final String PIPER_SCRIPT_RELATIVE_PATH =
        File.separator + "Runtime" + File.separator + "scripts" + File.separator + "system" + File.separator
            + "adaptors" + File.separator + "nio" + File.separator + "pipers" + File.separator;
    private static final String PIPE_SCRIPT_NAME = "bindings_piper.sh";
    private static final String PIPE_FILE_BASENAME = "pipe_";

    protected final String mirrorId;
    protected final int size;
    private final HashMap<String, PipePair> pipePool;
    protected final String basePipePath;

    private final MirrorMonitor monitor;

    private Process pipeBuilderProcess;
    private StreamGobbler pipeBuildeOutGobbler;
    private StreamGobbler pipeBuildeErrGobbler;

    private ControlPipePair pipeBuilderPipe;
    private ControlPipePair pipeWorkerPipe;


    /**
     * Piped Mirror constructor.
     * 
     * @param context Invocation context
     * @param size Processes size
     */
    public PipedMirror(InvocationContext context, int size) {
        this.mirrorId = String.valueOf(UUID.randomUUID().hashCode());
        String workingDir = context.getWorkingDir();
        if (!workingDir.endsWith(File.separator)) {
            workingDir += File.separator;
        }
        this.basePipePath = workingDir + PIPE_FILE_BASENAME + this.mirrorId + "_";
        this.size = size;
        this.pipePool = new HashMap<>();
        this.monitor = new MirrorMonitor();
    }

    public String getMirrorId() {
        return this.mirrorId;
    }

    protected final void init(InvocationContext context) {
        this.monitor.start();

        startPipeBuilder(context);

        if (Tracer.extraeEnabled()) {
            // long tracingHostId = context.getTracingHostID();
            Tracer.emitEvent(this.size, Tracer.getSyncType());
        }

        startWorker(context);
    }

    private void startPipeBuilder(InvocationContext context) {
        String installDir = context.getInstallDir();
        String piperScript = installDir + PIPER_SCRIPT_RELATIVE_PATH + PIPE_SCRIPT_NAME;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("PIPE Script: " + piperScript);
        }

        String args = constructPipeBuilderArgs(context);
        LOGGER.info("Init piper PipeBuilder");
        ProcessBuilder pb = new ProcessBuilder(piperScript, args);
        try {
            // Set NW environment
            Map<String, String> env = getEnvironment(context);

            env.put(COMPSsConstants.COMPSS_WORKING_DIR, context.getWorkingDir());
            env.put(COMPSsConstants.COMPSS_APP_DIR, context.getAppDir());

            pb.directory(new File(getPBWorkingDir(context)));
            pb.environment().putAll(env);

            // Clean the EXTRAE environment
            pb.environment().put("EXTRAE_SKIP_AUTO_LIBRARY_INITIALIZE", "1");
            pb.environment().remove(Tracer.LD_PRELOAD);
            pb.environment().remove(Tracer.EXTRAE_CONFIG_FILE);

            // Emit start host id event
            if (Tracer.extraeEnabled()) {
                // long tracingHostId = context.getTracingHostID();
                Tracer.emitEvent(this.size, Tracer.getSyncType());
            }

            // Launch process builder
            this.pipeBuilderProcess = pb.start();
            LOGGER.debug("Starting stdout/stderr gobblers ...");
            try {
                this.pipeBuilderProcess.getOutputStream().close();
            } catch (IOException e) {
                // Stream no Longer Exists
            }

            // Active wait until the process is created and the pipes are ready
            while (this.pipeBuilderProcess.isAlive() && !new File(this.pipeBuilderPipe.getOutboundPipe()).exists()) {
                // TODO: SHOULD WE ADD A TIMEOUT AT THIS POINT??
            }

            // Check that the process is still alive
            if (!this.pipeBuilderProcess.isAlive()) {
                ErrorManager.fatal(ERROR_PB_START);
            }

            // Enable pipes, monitor and commands
            this.pipeBuildeOutGobbler = new StreamGobbler(this.pipeBuilderProcess.getInputStream(), null, LOGGER);
            this.pipeBuildeErrGobbler = new StreamGobbler(this.pipeBuilderProcess.getErrorStream(), null, LOGGER);
            this.pipeBuildeOutGobbler.start();
            this.pipeBuildeErrGobbler.start();

            this.monitor.mainProcess(this.pipeBuilderProcess, this.pipeBuilderPipe);

            if (this.pipeBuilderPipe.sendCommand(new PingPipeCommand())) {
                try {
                    this.pipeBuilderPipe.waitForCommand(new PongPipeCommand());
                } catch (ClosedPipeException ie) {
                    ErrorManager.fatal("Exception creating process builder. Message: " + ie.getMessage(), ie);
                }
            } else {
                ErrorManager.fatal(ERROR_PB_START + ": Error sending first Ping command. ");
            }
        } catch (IOException e) {
            ErrorManager
                .error(ERROR_PB_START + ": Exception during process builder creation. Message: " + e.getMessage(), e);
        }
    }

    private String constructPipeBuilderArgs(InvocationContext context) {
        StringBuilder cmd = new StringBuilder();

        // Control pipes
        this.pipeBuilderPipe = new ControlPipePair(basePipePath, "control", this);
        cmd.append(this.pipeBuilderPipe.getOutboundPipe()).append(TOKEN_SEP);
        cmd.append(this.pipeBuilderPipe.getInboundPipe()).append(TOKEN_SEP);

        // Executor Pipes
        StringBuilder writePipes = new StringBuilder();
        StringBuilder readPipes = new StringBuilder();
        for (int i = 0; i < this.size; ++i) {
            String pipeName = "executor" + i;
            PipePair executorPipe = new PipePair(basePipePath, pipeName, this);
            this.pipePool.put(pipeName, executorPipe);
            writePipes.append(executorPipe.getOutboundPipe()).append(TOKEN_SEP);
            readPipes.append(executorPipe.getInboundPipe()).append(TOKEN_SEP);
        }

        // Write Pipes
        cmd.append(this.size).append(TOKEN_SEP);
        cmd.append(writePipes.toString());

        // Read Pipes
        cmd.append(this.size).append(TOKEN_SEP);
        cmd.append(readPipes.toString());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("WRITE PIPE Files: " + writePipes.toString() + "\n");
            LOGGER.debug("READ PIPE Files: " + readPipes.toString() + "\n");

            // Data pipes
            LOGGER.debug("WRITE DATA PIPE: " + pipeBuilderPipe + ".outbound");
            LOGGER.debug("READ DATA PIPE: " + pipeBuilderPipe + ".inbound");
        }

        cmd.append(Tracer.getLevel()).append(TOKEN_SEP);

        cmd.append(getPipeBuilderContext());

        // General Args are of the form: controlPipeW controlPipeR workerPipeW workerPipeR 2 pipeW1 pipeW2 2 pipeR1
        // pipeR2
        return cmd.toString();
    }

    public abstract String getPipeBuilderContext();

    private void startWorker(InvocationContext context) {
        this.pipeWorkerPipe = new ControlPipePair(this.basePipePath, "control_worker", this);

        String cmd = getLaunchWorkerCommand(context, this.pipeWorkerPipe);
        StartWorkerPipeCommand swpc = new StartWorkerPipeCommand(cmd, this.pipeWorkerPipe);
        if (this.pipeBuilderPipe.sendCommand(swpc)) {
            WorkerStartedPipeCommand startedCMD = new WorkerStartedPipeCommand();
            try {
                this.pipeBuilderPipe.waitForCommand(startedCMD);
            } catch (ClosedPipeException ie) {
                ErrorManager.fatal(ERROR_W_START);
            }
            int workerPID = startedCMD.getPid();
            this.monitor.registerWorker(this.mirrorId, workerPID, this.pipeWorkerPipe);
        } else {
            ErrorManager.fatal(ERROR_W_START);
        }
    }

    /**
     * Returns the launch command for every binding.
     *
     * @param context Invocation context
     * @param pipe Control pipes
     * @return
     */
    public abstract String getLaunchWorkerCommand(InvocationContext context, ControlPipePair pipe);

    /**
     * Returns the specific environment variables of each binding.
     *
     * @param context Invocation Context
     * @return Environment variables key, value map
     */
    public abstract Map<String, String> getEnvironment(InvocationContext context);

    protected String getPBWorkingDir(InvocationContext context) {
        return context.getWorkingDir();
    }

    @Override
    public void stop() {
        stopWorker();
        stopPiper();

        this.monitor.stop();
    }

    private void stopExecutors() {
        LOGGER.info("Stopping executor pipes for mirror " + this.mirrorId);

        for (String executorId : new LinkedList<>(pipePool.keySet())) {
            unregisterExecutor(executorId);
        }
    }

    private void stopWorker() {
        stopExecutors();

        LOGGER.info("Stopping mirror " + this.mirrorId);
        if (this.pipeWorkerPipe.sendCommand(new QuitPipeCommand())) {
            try {
                this.pipeWorkerPipe.waitForCommand(new QuitPipeCommand());
            } catch (ClosedPipeException cpe) {
                // Worker is already closed
            }
        } else {
            // Worker is already closed
        }

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getSyncType());
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            Tracer.emitEvent((long) timestamp.getTime(), Tracer.getSyncType());
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getSyncType());
        }
        this.monitor.unregisterWorker(this.mirrorId);
        this.pipeWorkerPipe.delete();
    }

    private void stopPiper() {
        LOGGER.info("Stopping piper process");

        boolean errorOnPipe = false;
        if (this.pipeBuilderPipe.sendCommand(new QuitPipeCommand())) {
            try {
                this.pipeBuilderPipe.waitForCommand(new QuitPipeCommand());
            } catch (ClosedPipeException cpe) {
                LOGGER.error(ERROR_W_PIPE, cpe);
                errorOnPipe = true;
            }
        } else {
            LOGGER.error(ERROR_W_PIPE);
            errorOnPipe = true;
        }

        try {
            if (errorOnPipe) {
                if (this.pipeBuilderProcess.isAlive()) {
                    LOGGER.info("Error passing QUIT message. Killing piper process");
                    this.pipeBuilderProcess.destroy();
                }
            } else {
                LOGGER.info("Waiting for finishing piper process");
                int exitCode = pipeBuilderProcess.waitFor();
                if (exitCode != 0) {
                    ErrorManager.error("ExternalExecutor piper ended with " + exitCode + " status");
                }
            }
            this.pipeBuildeOutGobbler.join();
            this.pipeBuildeErrGobbler.join();
        } catch (InterruptedException e) {
            // No need to handle such exception
        } finally {
            if (this.pipeBuilderProcess != null) {
                if (this.pipeBuilderProcess.getInputStream() != null) {
                    try {
                        this.pipeBuilderProcess.getInputStream().close();
                    } catch (IOException e) {
                        // No need to handle such exception
                    }
                }
                if (this.pipeBuilderProcess.getErrorStream() != null) {
                    try {
                        this.pipeBuilderProcess.getErrorStream().close();
                    } catch (IOException e) {
                        // No need to handle such exception
                    }
                }
            }
        }
        this.pipeBuilderPipe.delete();
        LOGGER.info("ExternalThreadPool finished");
    }

    @Override
    public PipePair registerExecutor(String executorId) {
        boolean createExecutor = false;
        PipePair pp;
        synchronized (this.pipePool) {
            pp = this.pipePool.get(executorId);
            if (pp == null) {
                pp = new PipePair(this.basePipePath, executorId, this);
                createExecutor = true;
                this.pipePool.put(executorId, pp);
            }
        }
        int executorPID = -1;
        if (createExecutor) {
            // Create pipe
            if (this.pipeBuilderPipe.sendCommand(new CreateChannelPipeCommand(pp))) {
                ChannelCreatedPipeCommand createdPipe = new ChannelCreatedPipeCommand(pp);
                try {
                    this.pipeBuilderPipe.waitForCommand(createdPipe);
                } catch (ClosedPipeException ie) {
                    throw new UnsupportedOperationException("Not yet implemented. Specific exception should be raised");
                }
            } else {
                throw new UnsupportedOperationException("Not yet implemented. Specific exception should be raised");
            }

            // Launch executor
            if (this.pipeWorkerPipe.sendCommand(new AddExecutorPipeCommand(pp))) {
                try {
                    AddedExecutorPipeCommand reply = new AddedExecutorPipeCommand(pp);
                    this.pipeWorkerPipe.waitForCommand(reply);
                    executorPID = reply.getPid();

                } catch (ClosedPipeException ie) {
                    throw new UnsupportedOperationException("Not yet implemented. Specific exception should be raised");
                }
            } else {
                throw new UnsupportedOperationException("Not yet implemented. Specific exception should be raised");
            }
        } else {
            // Query executor PID
            if (this.pipeWorkerPipe.sendCommand(new ExecutorPIDQueryPipeCommand(pp))) {
                try {
                    ExecutorPIDReplyPipeCommand reply = new ExecutorPIDReplyPipeCommand(pp);
                    this.pipeWorkerPipe.waitForCommand(reply);
                    executorPID = reply.getPids().get(0);
                } catch (ClosedPipeException ie) {
                    throw new UnsupportedOperationException("Not yet implemented. Specific exception should be raised");
                }
            } else {
                throw new UnsupportedOperationException("Not yet implemented. Specific exception should be raised");
            }
        }
        this.monitor.registerExecutor(this, executorId, executorPID, pp);

        return pp;
    }

    @Override
    public void unregisterExecutor(String executorId) {
        PipePair pp;
        synchronized (this.pipePool) {
            pp = this.pipePool.remove(executorId);
        }
        if (pp == null) {
            // Executor not alive on the mirror
            return;
        }

        // Shutting down executor
        if (pp.sendCommand(new QuitPipeCommand())) {
            try {
                PipeCommand command = null;
                while (command == null) {
                    command = pp.readCommand();
                    if (command.getType() != CommandType.QUIT) {
                        break;
                    }
                }
            } catch (ExternalExecutorException cpe) {
                // Executor is already dead -> Do nothing
            }
        }

        // Executor is down, no need to keep monitoring it
        this.monitor.unregisterExecutor(this, executorId);

        // Making sure that it is off through the Worker
        if (this.pipeWorkerPipe.sendCommand(new RemoveExecutorPipeCommand(pp))) {
            try {
                this.pipeWorkerPipe.waitForCommand(new RemovedExecutorPipeCommand(pp));
            } catch (ClosedPipeException cpe) {
                LOGGER.error(ERROR_W_PIPE, cpe);
            }
        } // else : Worker is dead and executor probably too since it is not responding. Ignore

        pp.delete();
        LOGGER.debug("EXECUTOR " + executorId + " shut down!");
    }

    @Override
    public void cancelJob(PipePair pipe) {
        // Making sure that it is off through the Worker
        this.pipeWorkerPipe.sendCommand(new CancelJobCommand(pipe));
    }
}
