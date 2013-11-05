/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

public class MovingAverageIntervalEstimator extends IntervalEstimator {
    final long intervals[];
    final int windowMagnitude;
    final int windowLength;
    final long windowMask;
    int currentPosition = 0;
    long count = 0;
    long intervalSum = 0;

    public MovingAverageIntervalEstimator(final int requestedWindowLength) {
        // Round window length up to nearest power of 2 to allow masking to work for modulus operation and
        // shifting to work for divide operations.
        this.windowMagnitude = (int) Math.ceil(Math.log(requestedWindowLength)/Math.log(2));
        this.windowLength = (int) Math.pow(2, windowMagnitude);
        this.windowMask = windowLength - 1;
        this.intervals = new long[this.windowLength];
    }

    public void recordInterval(long interval, long when) {
        intervalSum -= intervals[currentPosition];
        intervalSum += interval;
        intervals[currentPosition] = interval;
        count++;
        currentPosition = (int)(count & windowMask);
    }

    public long getEstimatedInterval(long when) {
        long averageInterval;
        if (count < windowLength) {
            averageInterval = intervalSum / count;
        } else {
            averageInterval = intervalSum >> windowMagnitude;
        }
        return averageInterval;
    }

    public long getCount() {
        return count;
    }
}
