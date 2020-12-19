package org.herring.nifi.processors.enums;

/**
 * List of supported strategies for interpreting null values in HDFS data for sqoop export
 */
public enum ExportNullInterpretationStrategy {
    SQOOP_DEFAULT,
    HIVE_DEFAULT,
    CUSTOM_VALUES;

    @Override
    public String toString() {
        switch (this) {
            case SQOOP_DEFAULT:
                return "SQOOP_DEFAULT";
            case HIVE_DEFAULT:
                return "HIVE_DEFAULT";
            case CUSTOM_VALUES:
                return "CUSTOM_VALUES";
        }
        return "";
    }
}