/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A moving average interval estimator. Estimates intervalEndTimes by averaging the interval values recorded in a
 * moving window. Will only provide average estimate once enough intervalEndTimes have been collected to fill the
 * window, and will return an impossibly long interval estimate until then.
 */
public class MovingAverageIntervalEstimator extends IntervalEstimator {
    final long intervalEndTimes[];
    final int windowMagnitude;
    final int windowLength;
    final long windowMask;
    AtomicLong count = new AtomicLong(0);
    volatile long forcedOrderingHelper;

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
        this.intervalEndTimes = new long[this.windowLength];
    }

    /**
     * @inheritDoc
     */
    @Override
    public void recordInterval(long when) {
        recordIntervalAndReturnWindowPosition(when);
    }

    int recordIntervalAndReturnWindowPosition(long when) {
        // Famous last words: This is racy only if enough in-flight recorders concurrently call this to wrap
        // around window while the first ones in are still in the call.
        // If windowLength is larger than largest possible number of concurrently recording threads, this should
        // be absolutely safe. I promise. ;-)
        long countAtSwapTime = count.getAndIncrement();
        int positionToSwap = (int)(countAtSwapTime & windowMask);
        intervalEndTimes[positionToSwap] = when;
        return positionToSwap;
    }

    /**
     * @inheritDoc
     */
    @Override
    public long getEstimatedInterval(long when) {
        long averageInterval;
        long sampledCount = count.get();
        if (sampledCount < windowLength) {
            return Long.MAX_VALUE;
        } else {
            int earliestWindowPosition = (int) (sampledCount & windowMask);
            int latestWindowPosition = (int) ((sampledCount + windowLength - 1) & windowMask);
            long windowStartTime = intervalEndTimes[earliestWindowPosition];
            forcedOrderingHelper = 0; // here to force ordering between reading of start and end times
            long windowEndTime = intervalEndTimes[latestWindowPosition];
            long windowTimeSpan = windowEndTime - windowStartTime;
            averageInterval = windowTimeSpan / (windowLength - 1);
        }
        return averageInterval;
    }

    int getCurrentPosition() {
        return (int) (count.get() & windowMask);
    }
}
