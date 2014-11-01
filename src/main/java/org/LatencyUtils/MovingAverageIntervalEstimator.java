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
    protected final long intervalEndTimes[];
    protected final int windowMagnitude;
    protected final int windowLength;
    protected final int windowMask;
    protected AtomicLong count = new AtomicLong(0);

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
        for (int i = 0; i < intervalEndTimes.length; i++) {
            intervalEndTimes[i] = Long.MIN_VALUE;
        }
    }

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

    @Override
    public long getEstimatedInterval(long when) {
        long sampledCount = count.get();

        if (sampledCount < windowLength) {
            return Long.MAX_VALUE;
        }

        long sampledCountPre;
        long windowTimeSpan;

        do {
            sampledCountPre = sampledCount;

            int earliestWindowPosition = (int) (sampledCount & windowMask);
            int latestWindowPosition = (int) ((sampledCount + windowLength - 1) & windowMask);
            long windowStartTime = intervalEndTimes[earliestWindowPosition];
            long windowEndTime = Math.max(intervalEndTimes[latestWindowPosition], when);
            windowTimeSpan = windowEndTime - windowStartTime;

            sampledCount = count.get();

            // Spin until we can have a stable count read during our calculation and the end time
            // represents an actually updated value (on a race where the count was updated and the
            // end time was not yet updated, the end time would be behind the start time, and
            // the time span would be negative).

        } while ((sampledCount != sampledCountPre) || (windowTimeSpan < 0));

        long averageInterval = windowTimeSpan / (windowLength - 1);

        // windowTimeSpan and averageInterval could theoretically be 0 if the entire window
        // was filled with the exact same nanotime sample. While that would truly indicate a
        // 0 interval within the window, we want to keep a minbar for the interval, as a 0 interval
        // can probably cause all sorts of interesting infinite loops in receiving logic.

        return Math.max(averageInterval, 1); // do not return a 0 interval estimate
    }

    protected int getCurrentPosition() {
        return (int) (count.get() & windowMask);
    }
}
