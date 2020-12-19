package org.herring.nifi.sqoop.enums;

/**
 * List of supported strategies for handling case where target HDFS directory exists
 */
public enum TargetHdfsDirExistsStrategy {
    DELETE_DIR_AND_IMPORT,
    FAIL_IMPORT;

    @Override
    public String toString() {
        switch (this) {
            case DELETE_DIR_AND_IMPORT:
                return "DELETE_DIR_AND_IMPORT";
            case FAIL_IMPORT:
                return "FAIL_IMPORT";
        }
        return "";
    }
}