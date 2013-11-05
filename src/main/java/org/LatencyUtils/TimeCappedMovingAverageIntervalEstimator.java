/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

public class TimeCappedMovingAverageIntervalEstimator extends MovingAverageIntervalEstimator {
    final long reportingTimes[];
    final long timeCap;

    public TimeCappedMovingAverageIntervalEstimator(final int windowLength, final long timeCap) {
        super(windowLength);
        reportingTimes = new long[windowLength];
        this.timeCap = timeCap;
    }

    public void recordInterval(long interval, long when) {
        reportingTimes[currentPosition] = when;
        super.recordInterval(interval, when);
    }

    public long getEstimatedInterval(long when) {
        if (when - timeCap > reportingTimes[currentPosition]) {
            // Earliest recorded position is not in the timeCap window. Window not up to date
            // enough, and we can't use it's estimated interval. Estimate as equal to the timeCap instead.
            return timeCap;
        }
        return super.getEstimatedInterval(when);
    }
}
