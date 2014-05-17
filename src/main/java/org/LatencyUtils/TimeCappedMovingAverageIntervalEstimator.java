/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A moving average interval estimator with a cap on the time window length that the moving window must completely
 * fit in in order to provide estimated time intervals.
 * <p>
 * A time capped interval estimator is useful for conservatively estimating time intervals in environments where rates
 * can change dramatically and semi-statically. For example, the rate of market rate updates seen just before market
 * close can be very high, dropping dramatically at market close and staying low thereafter. A non-time-capped
 * moving average estimator will project short estimated time interval long after market close, while a time capped
 * interval estimator will avoid carrying the small time interval beyond the time cap.
 * <p>
 * TimeCappedMovingAverageIntervalEstimator estimates time intervals by averaging the time interval values recorded
 * in a moving window, but if any of the results in the moving window occur outside of the capped time span
 * requested, only the results that fall within the time cap will be considered.
 * <p>
 * TimeCappedMovingAverageIntervalEstimator can react to pauses reported by an optional PauseDetector by temporarily
 * expanding the time cap to include each pause length, until such a time that the original time cap no longer overlaps
 * with the pause. It will also subtract the pause length from interval times measured across a detected pause. Providing a
 * pause detector is highly recommended, as without one the time cap can cause over-conservative interval estimation
 * (i.e. estimated interval times that are much higher than needed) in the presence of pauses.
 * <p>
 * All times and time units are in nanoseconds
 */

public class TimeCappedMovingAverageIntervalEstimator extends MovingAverageIntervalEstimator {
//    final long reportingTimes[];
    final long baseTimeCap;
    final PauseTracker pauseTracker;
    long timeCap;
    volatile long timeOfLastEstimatedInterval = 0;

    static final int maxPausesToTrack = 32;
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
    public void recordInterval(long when) {
        int position = super.recordIntervalAndReturnWindowPosition(when);
    }

    /**
     * @inheritDoc
     */
    @Override
    public synchronized long getEstimatedInterval(final long when) {
        timeOfLastEstimatedInterval = when;

        eliminateStalePauses(when);

        int numberOfWindowPositionsOutsideOfTimeCap = determineNumberOfWindowPositionsOutsideOfTimeCap(when);

        long windowStartTime = determineEarliestQualifyingTimeInWindow(when);

        long windowTimeSpan = when - windowStartTime;
        long totalPauseTimeInWindow = timeCap - baseTimeCap;
        int positionDelta = (windowLength - numberOfWindowPositionsOutsideOfTimeCap) - 1;

        if (positionDelta <= 0) {
            return Long.MAX_VALUE;
        }

        long averageInterval = (windowTimeSpan - totalPauseTimeInWindow)  / positionDelta;

        if (averageInterval <= 0) {
            return Long.MAX_VALUE;
        }

        return averageInterval;
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

    @Override
    public String toString() {
        long when = timeOfLastEstimatedInterval;

        eliminateStalePauses(when);

        int numberOfWindowPositionsOutsideOfTimeCap = determineNumberOfWindowPositionsOutsideOfTimeCap(when);

        long windowStartTime = determineEarliestQualifyingTimeInWindow(when);

        long windowTimeSpan = when - windowStartTime;
        long totalPauseTimeInWindow = timeCap - baseTimeCap;
        int positionDelta = (windowLength - numberOfWindowPositionsOutsideOfTimeCap) - 1;

        long averageInterval = Long.MAX_VALUE;

        if (positionDelta > 0) {
            averageInterval = (windowTimeSpan - totalPauseTimeInWindow)  / positionDelta;
        }

        return "IntervalEstimator: \n" +
                "Estimated Interval: " + getEstimatedInterval(when) + " (calculated at time " + when + ")\n" +
                "Time cap: " + timeCap + ", count = " + count.get() + ", currentPosition = " + getCurrentPosition() + "\n" +
                "timeCapStartTime = " + (when - timeCap) + ", numberOfWindowPositionsSkipped = " + numberOfWindowPositionsOutsideOfTimeCap + "\n" +
                "windowStartTime = " + windowStartTime + ", windowTimeSpan = " + windowTimeSpan + ", positionDelta = " + positionDelta + "\n" +
                "totalPauseTimeInWindow = " + totalPauseTimeInWindow + ", averageInterval = " + averageInterval + "\n";
    }

    private void eliminateStalePauses(final long when) {
        long newEarliestQualifyingTimeInWindow = determineEarliestQualifyingTimeInWindow(when);
        long earliestQualifyingTimeInWindow;
        do {
            earliestQualifyingTimeInWindow = newEarliestQualifyingTimeInWindow;

            long timeCapStartTime = when - timeCap;
            long earliestPauseTimeToConsider = Math.max(timeCapStartTime, earliestQualifyingTimeInWindow);

            long earliestPauseStartTime = pauseStartTimes.get(earliestPauseIndex);

            // Skip over and get rid of any pause records whose time has passed:
            while (earliestPauseStartTime < earliestPauseTimeToConsider) {
                // We just got past the start of this pause.

                // Reduce timeCap to skip over pause; recalculate timeCapStartTime and earliestPauseTimeToConsider:
                timeCap -= pauseLengths.get(earliestPauseIndex);
                timeCapStartTime = when - timeCap;
                earliestPauseTimeToConsider = Math.max(timeCapStartTime, earliestQualifyingTimeInWindow);

                // Erase pause record:
                pauseStartTimes.set(earliestPauseIndex, Long.MAX_VALUE);
                pauseLengths.set(earliestPauseIndex, 0);

                earliestPauseIndex = (earliestPauseIndex + 1) % maxPausesToTrack;
                earliestPauseStartTime = pauseStartTimes.get(earliestPauseIndex);
            }
            newEarliestQualifyingTimeInWindow = determineEarliestQualifyingTimeInWindow(when);
        } while (earliestQualifyingTimeInWindow != newEarliestQualifyingTimeInWindow);
    }

    private long determineEarliestQualifyingTimeInWindow(final long when) {
        int numberOfWindowPositionsOutsideOfTimeCap = determineNumberOfWindowPositionsOutsideOfTimeCap(when);
        if (numberOfWindowPositionsOutsideOfTimeCap == windowLength) {
            return Long.MAX_VALUE;
        }

        int earliestQualifyingWindowPosition =
                (int) ((getCurrentPosition() + numberOfWindowPositionsOutsideOfTimeCap) & windowMask);

        return intervalEndTimes[earliestQualifyingWindowPosition];
    }

    private int determineNumberOfWindowPositionsOutsideOfTimeCap(long when) {
        int currentPosition = getCurrentPosition();
        long timeCapStartTime = when - timeCap;

        // The common case will have a full window:
        if (intervalEndTimes[currentPosition] >= timeCapStartTime) {
            return 0;
        }

        // Binary search for a non-zero numberOfWindowPositionsOutsideOfTimeCap:
        // Start at a position half way to the end:

        int lowOffset = 0;
        int highOffset = windowLength;
        while (lowOffset < highOffset) {
            int currentGuessAtFirstQualifyingIndexOffset = (lowOffset + highOffset) >>> 1;
            int index = (currentPosition + currentGuessAtFirstQualifyingIndexOffset) & windowMask;

            long guessValue = intervalEndTimes[index];

            if (guessValue < timeCapStartTime) {
                // guess position is still outside of qualifying time range
                lowOffset = currentGuessAtFirstQualifyingIndexOffset + 1;
            } else {
                // guess position is inside qualifying time range
                highOffset = currentGuessAtFirstQualifyingIndexOffset;
            }
        }
        return lowOffset;  // value not found.
    }

    /**
     * PauseTracker is used to feed pause correction histograms whenever a pause is reported:
     */
    static class PauseTracker extends WeakReference<TimeCappedMovingAverageIntervalEstimator> implements PauseDetectorListener {
        final PauseDetector pauseDetector;

        PauseTracker(final PauseDetector pauseDetector, final TimeCappedMovingAverageIntervalEstimator estimator) {
            super(estimator);
            this.pauseDetector = pauseDetector;
            // Register as a high priority listener to make sure pauses are recorded with interval estimator
            // before they are reported to normal things that may call the estimators. This is intended to
            // ensure that the most recent pause lengths are correctly taken into account in interval estimates
            // taken immediately after a pause:
            pauseDetector.addListener(this, true /* high priority */);
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
