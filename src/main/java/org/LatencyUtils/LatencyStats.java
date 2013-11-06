/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import org.HdrHistogram.Histogram;

import java.lang.ref.WeakReference;
import java.util.Timer;
import java.util.TimerTask;

public class LatencyStats {
    static final long DEFAULT_LatencyUnitSizeInNsecs = 1;
    static final long DEFAULT_HighestTrackableLatency = 3600000000L;
    static final int DEFAULT_NumberOfSignificantValueDigits = 2;
    static final int DEFAULT_IntervalEstimatorWindowLength = 1024;
    static final long DEFAULT_HistogramIntervalLengthNsec = 1000000000L;
    static final int DEFAULT_numberOfRecentHistogramIntervalsToTrack = 1;

    final long highestTrackableLatency;
    final int numberOfSignificantValueDigits;

    final int intervalEstimatorWindowLength;

    final long histogramIntervalLengthNsec;
    final int numberOfRecentHistogramIntervalsToTrack;

    final PauseDetector pauseDetector;

    Histogram currentRecordingHistogram;

    Histogram currentPauseCorrectionHistogram;

    Histogram[] intervalRecordingHistograms;
    Histogram[] intervalPauseCorrectingHistograms;
    volatile int latestIntervalHistogramIndex = 0;

    Histogram uncorrectedAccumulatedHistogram;
    Histogram accumulatedHistogram;

    volatile long recordingStartEpoch = 0;
    volatile long recordingEndEpoch = 0;

    static final Timer latencyStatsTasksTimer = new Timer();
    final PeriodicHistogramUpdateTask updateTask;

    long previousRecordingTime;
    final IntervalEstimator intervalEstimator;

    static PauseDetector defaultPauseDetector;

    static void setDefaultPauseDetector(PauseDetector pauseDetector) {
        defaultPauseDetector = pauseDetector;
    }

    public LatencyStats() {
        this(defaultPauseDetector);
    }

    public LatencyStats(PauseDetector pauseDetector) {
        this(DEFAULT_HighestTrackableLatency, DEFAULT_NumberOfSignificantValueDigits,
                DEFAULT_HistogramIntervalLengthNsec, DEFAULT_numberOfRecentHistogramIntervalsToTrack,
                DEFAULT_IntervalEstimatorWindowLength, pauseDetector);
    }

    public LatencyStats(final long highestTrackableLatency, final int numberOfSignificantValueDigits,
                        final long histogramIntervalLengthNsec, final int numberOfRecentHistogramIntervalsToTrack,
                        final int intervalEstimatorWindowLength,
                        final PauseDetector pauseDetector) {

        if (pauseDetector == null) {
            this.pauseDetector = defaultPauseDetector;
        } else {
            this.pauseDetector = pauseDetector;
        }

        if (this.pauseDetector == null) {
            throw new IllegalArgumentException("Either a non-null pauseDetector argument must be supplied, or a default pause detector must be set for LatencyStats");
        }

        this.highestTrackableLatency = highestTrackableLatency;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        this.histogramIntervalLengthNsec = histogramIntervalLengthNsec;
        this.numberOfRecentHistogramIntervalsToTrack = numberOfRecentHistogramIntervalsToTrack;
        this.intervalEstimatorWindowLength = intervalEstimatorWindowLength;

        // Create alternating recording histograms:
        currentRecordingHistogram = new Histogram(highestTrackableLatency, numberOfSignificantValueDigits);
        intervalRecordingHistograms = new Histogram[numberOfRecentHistogramIntervalsToTrack];
        for (int i = 0; i < numberOfRecentHistogramIntervalsToTrack; i++) {
            intervalRecordingHistograms[i] = new Histogram(highestTrackableLatency, numberOfSignificantValueDigits);
        }

        // Create alternating pause correction histograms:
        currentPauseCorrectionHistogram = new Histogram(highestTrackableLatency, numberOfSignificantValueDigits);
        intervalPauseCorrectingHistograms = new Histogram[numberOfRecentHistogramIntervalsToTrack];
        for (int i = 0; i < numberOfRecentHistogramIntervalsToTrack; i++) {
            intervalPauseCorrectingHistograms[i] = new Histogram(highestTrackableLatency, numberOfSignificantValueDigits);
        }
        // Create accumulated Histograms:
        accumulatedHistogram = new Histogram(highestTrackableLatency, numberOfSignificantValueDigits);
        uncorrectedAccumulatedHistogram = new Histogram(highestTrackableLatency, numberOfSignificantValueDigits);

        // Create interval estimator:
        intervalEstimator = new MovingAverageIntervalEstimator(intervalEstimatorWindowLength);

        // Create and schedule periodic update task:
        updateTask = new PeriodicHistogramUpdateTask(histogramIntervalLengthNsec, this);

        // Create PauseTracker and register with pauseDetector:
        new PauseTracker(pauseDetector, this);
    }

    public void recordLatency(long latency) {
        recordingStartEpoch++; // Used to support otherwise un-synchronized histogram swapping
        trackRecordingInterval();
        currentRecordingHistogram.recordValue(latency);
        recordingEndEpoch++;
    }

    public synchronized Histogram getAccumulatedHistogram() {
        return accumulatedHistogram.copy();
    }

    public synchronized void getAccumulatedHistogramInto(Histogram targetHistogram) {
        accumulatedHistogram.copyInto(targetHistogram);
    }

    public synchronized void addAccumulatedHistogramTo(Histogram toHistogram) {
        toHistogram.add(accumulatedHistogram);
    }

    public synchronized Histogram getUncorrectedAccumulatedHistogram() {
        return uncorrectedAccumulatedHistogram.copy();
    }

    public synchronized Histogram getIntervalHistogram() {
        Histogram intervalHistogram = intervalRecordingHistograms[latestIntervalHistogramIndex].copy();
        intervalHistogram.add(intervalPauseCorrectingHistograms[latestIntervalHistogramIndex]);
        return intervalHistogram;
    }

    public synchronized void getIntervalHistogramInto(Histogram targetHistogram) {
        intervalRecordingHistograms[latestIntervalHistogramIndex].copyInto(targetHistogram);
        targetHistogram.add(intervalPauseCorrectingHistograms[latestIntervalHistogramIndex]);
    }

    public synchronized void addIntervalHistogramTo(Histogram toHistogram) {
        toHistogram.add(intervalRecordingHistograms[latestIntervalHistogramIndex]);
        toHistogram.add(intervalPauseCorrectingHistograms[latestIntervalHistogramIndex]);
    }

    public synchronized Histogram getUncorrectedIntervalHistogram() {
        return intervalRecordingHistograms[latestIntervalHistogramIndex].copy();
    }

    synchronized void recordDetectedPause(long pauseLengthNsec, long pauseEndTimeNsec) {
        if (intervalEstimator.getCount() > intervalEstimatorWindowLength) {
            long estimatedInterval =  intervalEstimator.getEstimatedInterval(pauseEndTimeNsec - pauseLengthNsec);
            if (pauseLengthNsec > estimatedInterval) {
                currentPauseCorrectionHistogram.recordValueWithExpectedInterval(pauseLengthNsec, estimatedInterval);
            }
        }
    }

    void trackRecordingInterval() {
        long now = System.nanoTime();
        long interval = now - previousRecordingTime;
        intervalEstimator.recordInterval(interval, now);
        previousRecordingTime = now;
    }

    void swapRecordingHistograms(int indexToSwap) {
        final Histogram tempHistogram = intervalRecordingHistograms[indexToSwap];
        intervalRecordingHistograms[indexToSwap] = currentRecordingHistogram;
        currentRecordingHistogram = tempHistogram;
    }

    void swapPauseCorrectionHistograms(int indexToSwap) {
        final Histogram tempHistogram = intervalPauseCorrectingHistograms[indexToSwap];
        intervalPauseCorrectingHistograms[indexToSwap] = currentPauseCorrectionHistogram;
        currentPauseCorrectionHistogram = tempHistogram;
    }

    synchronized void swapHistograms(int indexToSwap) {
        swapRecordingHistograms(indexToSwap);
        swapPauseCorrectionHistograms(indexToSwap);
    }

    synchronized void updateHistograms() {
        int indexToSwap = (latestIntervalHistogramIndex + 1) % numberOfRecentHistogramIntervalsToTrack;

        intervalRecordingHistograms[indexToSwap].reset();
        intervalPauseCorrectingHistograms[indexToSwap].reset();

        swapHistograms(indexToSwap);

        // Update the latest interval histogram index.
        latestIntervalHistogramIndex = indexToSwap;

        // Make sure we are not in the middle of recording a value on the previously current recording histogram:
        long startEpoch = recordingStartEpoch;
        while (recordingEndEpoch < startEpoch);

        uncorrectedAccumulatedHistogram.add(intervalRecordingHistograms[latestIntervalHistogramIndex]);
        accumulatedHistogram.add(intervalRecordingHistograms[latestIntervalHistogramIndex]);
        accumulatedHistogram.add(intervalPauseCorrectingHistograms[latestIntervalHistogramIndex]);
    }

    /**
     * PauseTracker is used to feed pause correction histograms whenever a pause is reported:
     */
    static class PauseTracker extends WeakReference<LatencyStats> implements PauseDetector.PauseDetectorListener {
        final PauseDetector pauseDetector;

        PauseTracker(final PauseDetector pauseDetector, final LatencyStats latencyStats) {
            super(latencyStats);
            this.pauseDetector = pauseDetector;
            // Register as listener:
            pauseDetector.addListener(this);
        }

        public void handlePauseEvent(final long pauseLengthNsec, final long pauseEndTimeNsec) {
            final LatencyStats latencyStats = this.get();

            if (latencyStats != null) {
                latencyStats.recordDetectedPause(pauseLengthNsec, pauseEndTimeNsec);
            } else {
                // Remove listener:
                pauseDetector.removeListener(this);
            }
        }
    }

    /**
     * PeriodicHistogramUpdateTask is used to collect interval and accumulated histograms with regular frequency:
     */
    static class PeriodicHistogramUpdateTask extends TimerTask {
        final WeakReference<LatencyStats> latencyStatsRef;

        PeriodicHistogramUpdateTask(final long histogramIntervalLengthNsec, final LatencyStats latencyStats) {
            this.latencyStatsRef = new WeakReference<LatencyStats>(latencyStats);
            latencyStatsTasksTimer.scheduleAtFixedRate(this, 0, (histogramIntervalLengthNsec / 1000000L));
        }

        public void run() {
            final LatencyStats latencyStats = latencyStatsRef.get();
            if (latencyStats != null) {
                latencyStats.updateHistograms();
            } else {
                this.cancel();
            }

        }
    }
}
