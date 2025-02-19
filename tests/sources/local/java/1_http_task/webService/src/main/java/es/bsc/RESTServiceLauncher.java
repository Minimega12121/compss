package es.bsc;

import java.util.concurrent.Semaphore;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import jakarta.ws.rs.Path;


/**
 * Handler for the REST Service Boot process.
 */

public class RESTServiceLauncher implements Runnable {

    private final int port;
    private final Semaphore sem;
    private Server server;
    private Exception startError = null;


    public RESTServiceLauncher(int port) {
        this.port = port;
        this.sem = new Semaphore(0);
    }

    @Override
    public void run() {
        Thread.currentThread().setName("REST Agent Service");

        try {
            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
            context.setContextPath("/");

            this.server = new Server(this.port);
            this.server.setHandler(context);

            ServletHolder jerseyServlet = context.addServlet(org.glassfish.jersey.servlet.ServletContainer.class, "/*");
            jerseyServlet.setInitOrder(0);

            jerseyServlet.setInitParameter("jersey.config.server.provider.classnames",
                testREST.class.getCanonicalName());
            try {
                this.server.start();
            } catch (Exception e) {
                this.startError = e;
                this.server.destroy();
                this.server = null;
            }
        } catch (Exception e) {
            this.startError = e;
            this.server = null;
        }
        this.sem.release();
        // If server is up keep running
        if (this.server != null) {
            try {
                this.server.join();
            } catch (Exception e) {
                // TODO: Send a notification to stop the Agent
            } finally {
                while (!server.isStopped()) {
                }
                this.server.destroy();
                this.server = null;
            }
        }
    }

    public Server getServer() {
        return this.server;
    }

    public Exception getStartError() {
        return this.startError;
    }

    /**
     * Waits for the service to be operative.
     *
     * @throws InterruptedException If the waiting semaphore is interrupted.
     */
    public void waitForBoot() throws InterruptedException {
        this.sem.acquire();
    }

}
