package vn.ds.study.mi.connector.gcs.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum Error {

    CONNECTION_ERROR("700101", "FILE:CONNECTION_ERROR"),
    ILLEGAL_PATH("700102", "FILE:ILLEGAL_PATH"),
    FILE_ALREADY_EXISTS("700103", "FILE:FILE_ALREADY_EXISTS"),
    RETRY_EXHAUSTED("700104", "FILE:RETRY_EXHAUSTED"),
    ACCESS_DENIED("700105", "FILE:ACCESS_DENIED"),
    FILE_LOCKING_ERROR("700106", "FILE:FILE_LOCKING_ERROR"),
    INVALID_CONFIGURATION("700107", "FILE:INVALID_CONFIGURATION"),
    OPERATION_ERROR("700108", "FILE:OPERATION_ERROR");

    private final String code;
    private final String message;
}
