/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import java.util.concurrent.atomic.AtomicLong;

public class MovingAverageIntervalEstimator extends IntervalEstimator {
    final long intervals[];
    final int windowMagnitude;
    final int windowLength;
    final long windowMask;
    AtomicLong count = new AtomicLong(0);
    AtomicLong intervalSum = new AtomicLong(0);

    public MovingAverageIntervalEstimator(final int requestedWindowLength) {
        // Round window length up to nearest power of 2 to allow masking to work for modulus operation and
        // shifting to work for divide operations.
        this.windowMagnitude = (int) Math.ceil(Math.log(requestedWindowLength)/Math.log(2));
        this.windowLength = (int) Math.pow(2, windowMagnitude);
        this.windowMask = windowLength - 1;
        this.intervals = new long[this.windowLength];
    }

    public int recordInterval(long interval, long when) {
        // Famous last words: This is racy only if enough in-flight recorders concurrently call this to wrap
        // around window while the first ones in are still in the call.
        // If windowLength is larger than largest possible number of concurrently recording threads, this should
        // be absolutely safe. I promise. ;-)
        long countAtSwapTime = count.getAndIncrement();
        int positionToSwap = (int)(countAtSwapTime & windowMask);
        intervalSum.addAndGet(interval - intervals[positionToSwap]);
        intervals[positionToSwap] = interval;
        return positionToSwap;
    }

    public long getEstimatedInterval(long when) {
        long averageInterval;
        long sampledCount = count.get();
        if (count.get() < windowLength) {
            averageInterval = intervalSum.get() / sampledCount;
        } else {
            averageInterval = intervalSum.get() >> windowMagnitude;
        }
        return averageInterval;
    }

    public long getCount() {
        return count.get();
    }

    int getCurrentPosition() {
        return (int) (count.get() & windowMask);
    }
}
