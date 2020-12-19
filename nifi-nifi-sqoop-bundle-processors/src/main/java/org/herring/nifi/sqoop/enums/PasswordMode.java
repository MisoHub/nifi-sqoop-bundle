package org.herring.nifi.sqoop.enums;

public enum PasswordMode {
    CLEAR_TEXT_ENTRY,
    ENCRYPTED_TEXT_ENTRY,
    ENCRYPTED_ON_HDFS_FILE;

    @Override
    public String toString() {
        switch (this) {
            case CLEAR_TEXT_ENTRY:
                return "CLEAR_TEXT_ENTRY";
            case ENCRYPTED_TEXT_ENTRY:
                return "ENCRYPTED_TEXT_ENTRY";
            case ENCRYPTED_ON_HDFS_FILE:
                return "ENCRYPTED_ON_HDFS_FILE";
        }
        return "";
    }
}