/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import java.lang.ref.WeakReference;

public class TimeCappedMovingAverageIntervalEstimator extends MovingAverageIntervalEstimator {
    final long reportingTimes[];
    final long baseTimeCap;
    final PauseTracker pauseTracker;
    long timeCap;

    static final int maxPausesToTrack = 32;
    long[] pauseStartTimes = new long[maxPausesToTrack];
    long[] pauseLengths = new long[maxPausesToTrack];
    int earliestPauseIndex = 0;
    int nextPauseRecordingIndex = 0;

    public TimeCappedMovingAverageIntervalEstimator(final int windowLength, final long timeCap, final PauseDetector pauseDetector) {
        super(windowLength);
        reportingTimes = new long[windowLength];
        this.baseTimeCap = timeCap;
        this.timeCap = baseTimeCap;
        this.pauseTracker = new PauseTracker(pauseDetector, this);
        for (int i = 0; i < maxPausesToTrack; i++) {
            pauseStartTimes[i] = Long.MAX_VALUE;
            pauseLengths[i] = 0;
        }
    }

    public int recordInterval(long interval, long when) {
        int position = super.recordInterval(interval, when);
        reportingTimes[position] = when;
        return position;
    }

    public synchronized long getEstimatedInterval(long when) {
        long timeCapStartTime = when - timeCap;

        // Skip over and get rid of any pause records whose time has passed:
        while (pauseStartTimes[earliestPauseIndex] < timeCapStartTime) {
            // We just got past the start of this pause.

            // Reduce timeCap to skip over pause:
            timeCap -= pauseLengths[earliestPauseIndex];

            // Erase pause record:
            pauseStartTimes[earliestPauseIndex] = Long.MAX_VALUE;
            pauseLengths[earliestPauseIndex] = 0;

            earliestPauseIndex = (earliestPauseIndex + 1) % maxPausesToTrack;
        }

        if (when - timeCap > reportingTimes[getCurrentPosition()]) {
            // Earliest recorded position is not in the timeCap window. Window not up to date
            // enough, and we can't use it's estimated interval. Estimate as equal to the timeCap instead.
            return timeCap;
        }
        return super.getEstimatedInterval(when);
    }

    synchronized void recordPause(long pauseLengthNsec, long pauseEndTimeNsec) {
        if (pauseStartTimes[nextPauseRecordingIndex] != Long.MAX_VALUE) {
            // We are overwriting a live pause record, account for it:
            timeCap -= pauseLengths[nextPauseRecordingIndex];
            earliestPauseIndex = (nextPauseRecordingIndex + 1) % maxPausesToTrack;
        }

        // extend timeCap to cover the pause:
        timeCap += pauseLengthNsec;

        // Track the pause so we can reduce the timeCap when it gets past the pause endTime:
        pauseStartTimes[nextPauseRecordingIndex] = pauseEndTimeNsec - pauseLengthNsec;
        pauseLengths[nextPauseRecordingIndex] = pauseLengthNsec;

        // Increment nextPauseRecordingIndex:
        nextPauseRecordingIndex = (nextPauseRecordingIndex + 1) % maxPausesToTrack;
    }

    public void stop() {
        pauseTracker.stop();
    }


    /**
     * PauseTracker is used to feed pause correction histograms whenever a pause is reported:
     */
    static class PauseTracker extends WeakReference<TimeCappedMovingAverageIntervalEstimator> implements PauseDetector.PauseDetectorListener {
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

        public void handlePauseEvent(final long pauseLengthNsec, final long pauseEndTimeNsec) {
            final TimeCappedMovingAverageIntervalEstimator estimator = this.get();

            if (estimator != null) {
                estimator.recordPause(pauseLengthNsec, pauseEndTimeNsec);
            } else {
                stop();
            }
        }
    }
}
