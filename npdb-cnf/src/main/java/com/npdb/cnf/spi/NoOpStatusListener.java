package com.npdb.cnf.spi;

/**
 * No-op listener used when the host process has not registered callbacks yet.
 */
public final class NoOpStatusListener implements StatusListener {

    public static final NoOpStatusListener INSTANCE = new NoOpStatusListener();

    private NoOpStatusListener() {
    }

    @Override
    public void onPoolStateChanged(String poolId, String newState, String reason) {
        // no-op
    }

    @Override
    public void onSnapshot(com.npdb.cnf.status.PoolStatusSnapshot snapshot) {
        // no-op
    }

    @Override
    public void onAlarm(AlarmSeverity severity, String poolId, String message) {
        // no-op
    }
}
