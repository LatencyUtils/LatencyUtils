/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

public abstract class IntervalEstimator {

    abstract public void recordInterval(long interval, long when);

    abstract public long getEstimatedInterval(long when);

    abstract public long getCount();

}
