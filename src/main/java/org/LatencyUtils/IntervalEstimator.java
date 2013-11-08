/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

/**
 * IntervalEstimator is used to estimate intervals, potentially based on observed intervals recorded in it.
 */
public abstract class IntervalEstimator {

    /**
     * Record an interval
     * @param interval the interval to record (in nanoseconds)
     * @param when the end time (in nanoTime units) at which the interval was observed.
     */
    abstract public void recordInterval(long interval, long when);

    /**
     * Provides the estimated interval
     *
     * @param when the time (preferably now) at which the estimated interval is requested.
     * @return estimated interval
     */
    abstract public long getEstimatedInterval(long when);
}
