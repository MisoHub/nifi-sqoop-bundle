package org.herring.nifi.sqoop;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.herring.nifi.sqoop.enums.PasswordMode;

/**
 * Interface for a connection provider service for Sqoop<br>
 * Provides information for connecting to a relational system
 */
@Tags({"ingest", "sqoop", "rdbms", "database", "table"})
@CapabilityDescription("Provides information for connecting to a relational system for running a sqoop job")
public interface SqoopConnectionService extends ControllerService {

    /**
     * Connection string to access source system
     *
     * @return the connection string
     */

    String getConnectionString();

    /**
     * User name for source system authentication
     *
     * @return the user name
     */
    String getUserName();

    /**
     * Get password mode
     *
     * @return the password mode
     */
    PasswordMode getPasswordMode();

    /**
     * Location of encrypted password file on HDFS
     *
     * @return the encrypted password file location in HDFS
     */
    String getPasswordHdfsFile();

    /**
     * Passphrase to decrypt password
     *
     * @return the pass phrase
     */
    String getPasswordPassphrase();

    /**
     * Gets the password
     *
     * @return the password
     */
    String getEnteredPassword();

    /**
     * Connection manager to access source system
     *
     * @return the connection manager string
     */
    String getConnectionManager();

    /**
     * Driver to access source system
     *
     * @return the driver string
     */
    String getDriver();
}