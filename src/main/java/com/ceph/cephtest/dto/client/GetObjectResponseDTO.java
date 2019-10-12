package com.ceph.cephtest.dto.client;

import java.util.List;

public class GetObjectResponseDTO {
    private List<String> directoryNameList;
    private List<ObjectInfoResponseDTO> objectInfoResponseDTOList;

    public GetObjectResponseDTO(List<String> directoryNameList, List<ObjectInfoResponseDTO> objectInfoResponseDTOList) {
        this.directoryNameList = directoryNameList;
        this.objectInfoResponseDTOList = objectInfoResponseDTOList;
    }

    public List<String> getDirectoryNameList() {
        return directoryNameList;
    }

    public void setDirectoryNameList(List<String> directoryNameList) {
        this.directoryNameList = directoryNameList;
    }

    public List<ObjectInfoResponseDTO> getObjectInfoResponseDTOList() {
        return objectInfoResponseDTOList;
    }

    public void setObjectInfoResponseDTOList(List<ObjectInfoResponseDTO> objectInfoResponseDTOList) {
        this.objectInfoResponseDTOList = objectInfoResponseDTOList;
    }
}
