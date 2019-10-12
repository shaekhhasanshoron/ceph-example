package com.ceph.cephtest.dto.client;

public class CopyMoveDirectoryObjectHelperDTO {
    private String objectFullKeyName;
    private String objectKeyFromCopiedDirectory;
    private String copiedDirectoryName;
    private String objectName;

    public CopyMoveDirectoryObjectHelperDTO() {
    }

    public CopyMoveDirectoryObjectHelperDTO(String objectFullKeyName, String objectKeyFromCopiedDirectory, String copiedDirectoryName, String objectName) {
        this.objectFullKeyName = objectFullKeyName;
        this.objectKeyFromCopiedDirectory = objectKeyFromCopiedDirectory;
        this.copiedDirectoryName = copiedDirectoryName;
        this.objectName = objectName;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public String getCopiedDirectoryName() {
        return copiedDirectoryName;
    }

    public void setCopiedDirectoryName(String copiedDirectoryName) {
        this.copiedDirectoryName = copiedDirectoryName;
    }

    public String getObjectFullKeyName() {
        return objectFullKeyName;
    }

    public void setObjectFullKeyName(String objectFullKeyName) {
        this.objectFullKeyName = objectFullKeyName;
    }

    public String getObjectKeyFromCopiedDirectory() {
        return objectKeyFromCopiedDirectory;
    }

    public void setObjectKeyFromCopiedDirectory(String objectKeyFromCopiedDirectory) {
        this.objectKeyFromCopiedDirectory = objectKeyFromCopiedDirectory;
    }
}
