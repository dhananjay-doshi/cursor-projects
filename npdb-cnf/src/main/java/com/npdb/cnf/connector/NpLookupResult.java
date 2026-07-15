package com.npdb.cnf.connector;

/**
 * Result of an NP subscriber lookup.
 */
public final class NpLookupResult {

    private final String subscriberNumber;
    private final String routingNumber;
    private final String poolId;
    private final boolean found;

    private NpLookupResult(String subscriberNumber, String routingNumber, String poolId, boolean found) {
        this.subscriberNumber = subscriberNumber;
        this.routingNumber = routingNumber;
        this.poolId = poolId;
        this.found = found;
    }

    public static NpLookupResult found(String subscriberNumber, String routingNumber, String poolId) {
        return new NpLookupResult(subscriberNumber, routingNumber, poolId, true);
    }

    public static NpLookupResult notFound(String subscriberNumber, String poolId) {
        return new NpLookupResult(subscriberNumber, null, poolId, false);
    }

    public String subscriberNumber() {
        return subscriberNumber;
    }

    public String routingNumber() {
        return routingNumber;
    }

    public String poolId() {
        return poolId;
    }

    public boolean found() {
        return found;
    }
}
