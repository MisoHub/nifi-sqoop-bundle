package org.herring.nifi.sqoop.process;
import org.apache.nifi.logging.ComponentLog;
import org.herring.nifi.sqoop.security.KerberosConfig;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.google.gson.internal.$Gson$Preconditions.checkNotNull;

/**
 * Run a sqoop export job via a system process
 */
public class SqoopExportProcessRunner {

    private List<String> commands = new ArrayList<>();
    private KerberosConfig kerberosConfig = null;

    private ComponentLog logger = null;

    private CountDownLatch latch;
    private String[] logLines;

    /**
     * Constructor
     *
     * @param kerberosConfig kerberos configuration
     * @param commands       command list to run
     * @param logger         logger
     */
    public SqoopExportProcessRunner(KerberosConfig kerberosConfig,
                                    final List<String> commands,
                                    ComponentLog logger) {
        if (commands == null) {
            throw new IllegalArgumentException("Unable to create SqoopExportProcessRunner with null arguments");
        }

        checkNotNull(kerberosConfig);
        checkNotNull(commands);
        checkNotNull(logger);

        this.kerberosConfig = kerberosConfig;
        this.commands = commands;
        this.logger = logger;

        latch = new CountDownLatch(1);
        logLines = new String[1];

        this.logger.info("SqoopExportProcessRunner initialized.");
    }

    /**
     * Execute the process
     *
     * @return result of execution as {@link SqoopProcessResult}
     */
    public SqoopProcessResult execute() {
        logger.info("Executing sqoop export command");
        int exitValue = -1;

        try {
            ProcessBuilder pb = new ProcessBuilder(commands);

            if (kerberosConfig.isKerberosConfigured()) {
                logger.info("Kerberos service principal and keytab are provided.");
                ProcessBuilder processBuilderKerberosInit = new ProcessBuilder(kerberosConfig.getKinitCommandAsList());
                Process processKerberosInit = processBuilderKerberosInit.start();
                int kerberosInitExitValue = processKerberosInit.waitFor();
                if (kerberosInitExitValue != 0) {
                    logger.error("Kerberos kinit failed ({})", new Object[]{kerberosConfig.getKinitCommandAsString()});
                    throw new Exception("Kerberos kinit failed");
                } else {
                    logger.info("Kerberos kinit succeeded");
                }
            }

            Process process = pb.start();

            InputStream inputStream = process.getInputStream();
            InputStream errorStream = process.getErrorStream();

            SqoopExportThreadedStreamHandler inputStreamHandler = new SqoopExportThreadedStreamHandler(inputStream, logger, logLines, latch);
            SqoopExportThreadedStreamHandler errorStreamHandler = new SqoopExportThreadedStreamHandler(errorStream, logger, logLines, latch);

            inputStreamHandler.start();
            errorStreamHandler.start();

            logger.info("Waiting for sqoop export job to complete");
            latch.await();
            exitValue = process.waitFor();

            inputStreamHandler.interrupt();
            errorStreamHandler.interrupt();

            inputStreamHandler.join();
            errorStreamHandler.join();

            logger.info("Sqoop export job completed");

            return new SqoopProcessResult(exitValue, logLines);
        } catch (Exception e) {
            logger.error("Error running sqoop export command [{}].", new Object[]{e.getMessage()});

            for (long i = 0; i < latch.getCount(); i++) {
                latch.countDown();
            }
            return new SqoopProcessResult(exitValue, logLines);
        }
    }

    /*
    Get the full command to be executed
     */
    @SuppressWarnings("unused")
    private String getFullCommand() {

        StringBuffer retVal = new StringBuffer();
        for (String c : commands) {
            retVal.append(c);
            retVal.append(" ");
        }
        return retVal.toString();
    }
}

