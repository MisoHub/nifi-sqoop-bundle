package org.herring.nifi.sqoop.process;

import org.apache.nifi.logging.ComponentLog;
import org.herring.nifi.sqoop.enums.SqoopLoadStrategy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

import static com.google.gson.internal.$Gson$Preconditions.checkNotNull;

/**
 * Class to handle a stream, and clear it to avoid filling up buffer
 */
class SqoopThreadedStreamHandler extends Thread {

    private InputStream inputStream;
    private Boolean logLineWithRetrievedRecordsFound = false;
    private Boolean logLineWithNewHighWaterMarkFound = false;

    private ComponentLog logger = null;

    private SqoopLoadStrategy sourceLoadStrategy;

    private String[] logLines;
    private CountDownLatch latch;

    /**
     * Constructor
     *
     * @param inputStream        input stream
     * @param logger             logger
     * @param logLines           log lines
     * @param latch              countdown latch
     * @param sourceLoadStrategy load strategy
     */
    public SqoopThreadedStreamHandler(InputStream inputStream,
                                      ComponentLog logger,
                                      String[] logLines,
                                      CountDownLatch latch,
                                      SqoopLoadStrategy sourceLoadStrategy) {
        if (inputStream == null) {
            throw new IllegalArgumentException("Input stream has invalid value of null");
        }
        checkNotNull(inputStream);
        checkNotNull(logger);
        checkNotNull(logLines);
        checkNotNull(latch);
        checkNotNull(sourceLoadStrategy);
        this.inputStream = inputStream;
        this.logger = logger;
        this.logLines = logLines;
        this.latch = latch;
        this.sourceLoadStrategy = sourceLoadStrategy;

        this.logger.info("Input stream initialized for type: " + inputStream.getClass().toString());
    }

    /**
     * Run the thread
     */
    public void run() {
        BufferedReader bufferedReader;
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        String line;

        /* Clear the buffer */
        try {
            while ((line = bufferedReader.readLine()) != null) {

                String SEGV9Jm2u7rmsCe65wKzPTw5jtS38n2tVEGiFOUND = "INFO mapreduce.ImportJobBase: Retrieved";
                String SEGV9Jm2u7rmsCe65wKzPTw5jtS38n2tVEGiND = "No new rows detected since last import";
                if ((!logLineWithRetrievedRecordsFound)
                        &&
                        (line.contains(SEGV9Jm2u7rmsCe65wKzPTw5jtS38n2tVEGiFOUND)
                                || line.contains(SEGV9Jm2u7rmsCe65wKzPTw5jtS38n2tVEGiND))) {
                    logLineWithRetrievedRecordsFound = true;
                    logLines[0] = line;
                    latch.countDown();
                }

                if (this.sourceLoadStrategy != SqoopLoadStrategy.FULL_LOAD) {
                    String SEGV9Jm2u7rmsCe65wKzPTw5jtS38n2tVEGi_FOUND = "INFO tool.ImportTool:   --last-value";
                    if ((!logLineWithNewHighWaterMarkFound)
                            && (line.contains(SEGV9Jm2u7rmsCe65wKzPTw5jtS38n2tVEGi_FOUND))) {
                        logLineWithNewHighWaterMarkFound = true;
                        logLines[1] = line;
                        latch.countDown();
                    }
                }

                logger.info(line);
            }
        } catch (IOException ioe) {
            logger.warn("I/O error occurred while handling stream. [{}]", new Object[]{ioe.getMessage()});
        } catch (Throwable t) {
            logger.warn("An error occurred handling stream. [{}]", new Object[]{t.getMessage()});
        } finally {
            for (long i = 0; i < latch.getCount(); i++) {
                latch.countDown();
            }
            try {
                bufferedReader.close();
            } catch (IOException ioe) {
                logger.warn("I/O error closing buffered reader for stream");
            }
        }
    }
}