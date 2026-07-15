package com.npdb.cnf.connector;

/**
 * Failures from the DB connector.
 */
public class NpdbException extends RuntimeException {

    public enum Code {
        ALL_POOLS_UNAVAILABLE,
        QUERY_FAILED,
        ACQUIRE_TIMEOUT,
        CONFIG,
        INTERRUPTED
    }

    private final Code code;
    private final String poolId;

    public NpdbException(Code code, String message) {
        this(code, message, null, null);
    }

    public NpdbException(Code code, String message, String poolId, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.poolId = poolId;
    }

    public Code code() {
        return code;
    }

    public String poolId() {
        return poolId;
    }
}
