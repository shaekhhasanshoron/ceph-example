package com.ceph.cephtest;

import com.ceph.cephtest.enums.ResponseStatus;

public class ResponseDTO<T> extends BaseResponseDTO {

    private T data;

    public ResponseDTO() {
    }

    public ResponseDTO(T data) {
        this.data = data;
    }

    public ResponseDTO(T data, String message) {
        this.data = data;
        this.setMessage(message);
    }

    public ResponseDTO(T data, ResponseStatus status, String message) {
        this.data = data;
        this.setStatus(status);
        this.setMessage(message);
    }

    public ResponseDTO generateSuccessResponse(T data) {
        this.data = data;
        this.setStatus(ResponseStatus.success);
        this.setMessage("");
        return this;
    }

    public ResponseDTO generateSuccessResponse(T data, String message) {
        this.data = data;
        this.setStatus(ResponseStatus.success);
        this.setMessage(message);
        return this;
    }

    public ResponseDTO generateErrorResponse() {
        this.data = null;
        this.setStatus(ResponseStatus.error);
        this.setMessage("Unknown error");
        return this;
    }

    public ResponseDTO generateErrorResponse(String message) {
        this.data = null;
        this.setStatus(ResponseStatus.error);
        this.setMessage(message);
        return this;
    }

    public ResponseDTO generateWarningResponse(T data) {
        this.data = data;
        this.setStatus(ResponseStatus.warning);
        this.setMessage("");
        return this;
    }

    public ResponseDTO generateWarningResponse(T data, String message) {
        this.data = data;
        this.setStatus(ResponseStatus.warning);
        this.setMessage(message);
        return this;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

}
