/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A moving average interval estimator. Estimates intervals by averaging the interval values recorded in a
 * moving window. Will only provide average estimate once enough intervals have been collected to fill the
 * window, and will return an impossibly long interval estimate until then.
 */
public class MovingAverageIntervalEstimator extends IntervalEstimator {
    final long intervals[];
    final int windowMagnitude;
    final int windowLength;
    final long windowMask;
    AtomicLong count = new AtomicLong(0);
    AtomicLong intervalSum = new AtomicLong(0);

    /**
     *
     * @param requestedWindowLength The requested length of the moving window. May be rounded up to nearest power of 2.
     */
    public MovingAverageIntervalEstimator(final int requestedWindowLength) {
        // Round window length up to nearest power of 2 to allow masking to work for modulus operation and
        // shifting to work for divide operations.
        this.windowMagnitude = (int) Math.ceil(Math.log(requestedWindowLength)/Math.log(2));
        this.windowLength = (int) Math.pow(2, windowMagnitude);
        this.windowMask = windowLength - 1;
        this.intervals = new long[this.windowLength];
    }

    /**
     * @inheritDoc
     */
    @Override
    public void recordInterval(long interval, long when) {
        recordIntervalAndReturnWindowPosition(interval, when);
    }

    int recordIntervalAndReturnWindowPosition(long interval, long when) {
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

    /**
     * @inheritDoc
     */
    @Override
    public long getEstimatedInterval(long when) {
        long averageInterval;
        long sampledCount = count.get();
        if (count.get() < windowLength) {
            return Long.MAX_VALUE;
        } else {
            averageInterval = intervalSum.get() >> windowMagnitude;
        }
        return averageInterval;
    }

    int getCurrentPosition() {
        return (int) (count.get() & windowMask);
    }
}
