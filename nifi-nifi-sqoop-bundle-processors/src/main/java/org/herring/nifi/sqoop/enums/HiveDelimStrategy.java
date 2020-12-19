package org.herring.nifi.sqoop.enums;
/**
 * List of supported strategies for handling Hive-specific delimiters (\n, \r, \01)
 */
public enum HiveDelimStrategy {
    DROP,
    KEEP,
    REPLACE;

    @Override
    public String toString() {
        switch (this) {
            case DROP:
                return "DROP";
            case KEEP:
                return "KEEP";
            case REPLACE:
                return "REPLACE";
        }
        return "";
    }
}
