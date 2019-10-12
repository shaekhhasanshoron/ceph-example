package com.ceph.cephtest;

import com.ceph.cephtest.enums.ResponseStatus;
import com.ceph.cephtest.interfaces.BaseOutput;

public class BaseResponseDTO implements BaseOutput {
    private ResponseStatus status;
    private String message;

    public BaseResponseDTO() {
    }

    public BaseResponseDTO(ResponseStatus status, String message) {
        this.status = status;
        this.message = message;
    }

    public ResponseStatus getStatus() {
        return status;
    }

    public void setStatus(ResponseStatus status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
