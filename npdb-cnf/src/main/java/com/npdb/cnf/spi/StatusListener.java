package com.npdb.cnf.spi;

import com.npdb.cnf.status.PoolStatusSnapshot;

/**
 * Alarm / SLP broadcast hooks implemented by the embedding application.
 */
public interface StatusListener {

    void onPoolStateChanged(String poolId, String newState, String reason);

    void onSnapshot(PoolStatusSnapshot snapshot);

    void onAlarm(AlarmSeverity severity, String poolId, String message);

    enum AlarmSeverity {
        CLEAR,
        MAJOR,
        CRITICAL
    }
}
