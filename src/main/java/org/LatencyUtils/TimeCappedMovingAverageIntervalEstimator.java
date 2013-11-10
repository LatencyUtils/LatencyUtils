/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A moving average interval estimator with a cap on the time window length that the moving window must completely
 * fit in in order to provide estimated intervals.
 *
 * A time capped interval estimator is useful for conservatively estimating intervals in environments where rates
 * can change dramatically and semi=statically. For example, the rate of market rate updates seen just before market
 * close can be very high, dropping dramatically at market close and staying low thereafter. A non-time-capped
 * moving average estimator will project short estimated intervals long after market close, while a time capped
 * interval estimator will avoid carrying the small intervals beyond the time cap.
 *
 * TimeCappedMovingAverageIntervalEstimator Estimates intervals by averaging the interval values recorded in a
 * moving window, but if any of the results in the moving window occur outside of the capped time span requested an
 * impossibly long interval will be provided instead.
 * <p>
 * TimeCappedMovingAverageIntervalEstimator can react to pauses reported by an optional PauseDetector by temporarily
 * expanding the time cap to include each pause length, until such a time that the original time cap no longer overlaps
 * with the pause. It will also subtract the pause length form intervals measured across a detected pause. Providing a
 * pause detector is highly recommended, as without one the time cap can cause over-conservative interval estimation
 * (i.e. estimated intervals that are much higher than needed) in the presence of pauses.
 * <p>
 * All times and time units are in nanoseconds
 */

public class TimeCappedMovingAverageIntervalEstimator extends MovingAverageIntervalEstimator {
    final long reportingTimes[];
    final long baseTimeCap;
    final PauseTracker pauseTracker;
    long timeCap;

    static final int maxPausesToTrack = 16;
    volatile long latestPauseStartTime = 0;
    AtomicLongArray pauseStartTimes = new AtomicLongArray(maxPausesToTrack);
    AtomicLongArray pauseLengths = new AtomicLongArray(maxPausesToTrack);
    int earliestPauseIndex = 0;
    int nextPauseRecordingIndex = 0;

    /**
     *
     * @param requestedWindowLength The requested length of the moving window. May be rounded up to nearest
     *                              power of 2.
     * @param timeCap The cap on time span length (in nanosecond units) in which all window results must fit
     *                in order for average estimate to be provided
     */
    public TimeCappedMovingAverageIntervalEstimator(final int requestedWindowLength, final long timeCap) {
        this(requestedWindowLength, timeCap, null);
    }

    /**
     *
     * @param requestedWindowLength The requested length of the moving window. May be rounded up to nearest
     *                              power of 2.
     * @param timeCap The cap on time span length in which all window results must fit in order for average
     *                estimate to be provided
     * @param pauseDetector The PauseDetector to use to track pauses
     */
    public TimeCappedMovingAverageIntervalEstimator(final int requestedWindowLength, final long timeCap, final PauseDetector pauseDetector) {
        super(requestedWindowLength);
        reportingTimes = new long[windowLength];
        this.baseTimeCap = timeCap;
        this.timeCap = timeCap;
        if (pauseDetector != null) {
            this.pauseTracker = new PauseTracker(pauseDetector, this);
        } else {
            pauseTracker = null;
        }
        for (int i = 0; i < maxPausesToTrack; i++) {
            pauseStartTimes.set(i, Long.MAX_VALUE);
            pauseLengths.set(i, 0);
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void recordInterval(long interval, long when) {
        long intervalStartTime = when - interval;

        // If interval overlaps with any pauses, we'll need to subtract the overlapping pause times:
        if (intervalStartTime < latestPauseStartTime) {
            int pauseIndex = earliestPauseIndex;
            int lastPauseIndex = (earliestPauseIndex + maxPausesToTrack - 1) % maxPausesToTrack;
            long pauseStartTimeAtIndex = pauseStartTimes.get(pauseIndex);
            long overlappingPauseLengths = 0;
            while ( (pauseIndex != lastPauseIndex) &&
                    (pauseStartTimeAtIndex != Long.MAX_VALUE) &&
                    (intervalStartTime < pauseStartTimeAtIndex)
                    ) {
                overlappingPauseLengths += pauseLengths.get(pauseIndex);
                pauseIndex = (pauseIndex + 1) % maxPausesToTrack;
                pauseStartTimeAtIndex = pauseStartTimes.get(pauseIndex);
            }
            // reduce interval by overlapping pause lengths:
            interval -= overlappingPauseLengths;
        }

        int position = super.recordIntervalAndReturnWindowPosition(interval, when);
        reportingTimes[position] = when;
    }

    /**
     * @inheritDoc
     */
    @Override
    public synchronized long getEstimatedInterval(final long when) {
        long timeCapStartTime = when - timeCap;

        // Skip over and get rid of any pause records whose time has passed:
        while (pauseStartTimes.get(earliestPauseIndex) < timeCapStartTime) {
            // We just got past the start of this pause.

            // Reduce timeCap to skip over pause:
            timeCap -= pauseLengths.get(earliestPauseIndex);

            // Erase pause record:
            pauseStartTimes.set(earliestPauseIndex, Long.MAX_VALUE);
            pauseLengths.set(earliestPauseIndex, 0);

            earliestPauseIndex = (earliestPauseIndex + 1) % maxPausesToTrack;
        }

        if (when - timeCap > reportingTimes[getCurrentPosition()]) {
            // Earliest recorded position is not in the timeCap window. Window not up to date
            // enough, and we can't use it's estimated interval. Estimate as impossibly big number.
            return Long.MAX_VALUE;
        }

        return super.getEstimatedInterval(when);
    }

    synchronized void recordPause(final long pauseLength, final long pauseEndTime) {
        latestPauseStartTime = pauseEndTime - pauseLength;

        if (pauseStartTimes.get(nextPauseRecordingIndex) != Long.MAX_VALUE) {
            // We are overwriting a live pause record, account for it:
            timeCap -= pauseLengths.get(nextPauseRecordingIndex);
            earliestPauseIndex = (nextPauseRecordingIndex + 1) % maxPausesToTrack;
        }

        // extend timeCap to cover the pause:
        timeCap += pauseLength;

        // Track the pause so we can reduce the timeCap when it gets past the pause endTime:
        pauseStartTimes.set(nextPauseRecordingIndex, (pauseEndTime - pauseLength));
        pauseLengths.set(nextPauseRecordingIndex, pauseLength);

        // Increment nextPauseRecordingIndex:
        nextPauseRecordingIndex = (nextPauseRecordingIndex + 1) % maxPausesToTrack;
    }

    /**
     * Stop the tracking via the pauseDetector, and remove this estimator from the pause detector's listeners.
     */
    public void stop() {
        if (pauseTracker != null) {
            pauseTracker.stop();
        }
    }


    /**
     * PauseTracker is used to feed pause correction histograms whenever a pause is reported:
     */
    static class PauseTracker extends WeakReference<TimeCappedMovingAverageIntervalEstimator> implements PauseDetectorListener {
        final PauseDetector pauseDetector;

        PauseTracker(final PauseDetector pauseDetector, final TimeCappedMovingAverageIntervalEstimator estimator) {
            super(estimator);
            this.pauseDetector = pauseDetector;
            // Register as listener:
            pauseDetector.addListener(this);
        }

        public void stop() {
            pauseDetector.removeListener(this);
        }

        public void handlePauseEvent(final long pauseLength, final long pauseEndTime) {
            final TimeCappedMovingAverageIntervalEstimator estimator = this.get();

            if (estimator != null) {
                estimator.recordPause(pauseLength, pauseEndTime);
            } else {
                stop();
            }
        }
    }
}
