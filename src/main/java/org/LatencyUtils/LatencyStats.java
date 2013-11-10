/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.AtomicHistogram;

import java.lang.ref.WeakReference;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class LatencyStats {
    // All times and time units are in nanoseconds
    
    static final long DEFAULT_HighestTrackableLatency = 3600000000000L;
    static final int DEFAULT_NumberOfSignificantValueDigits = 2;

    static final int DEFAULT_IntervalEstimatorWindowLength = 1024;

    static final long DEFAULT_HistogramUpdateInterval = 1000000000L;

    static final int DEFAULT_numberOfRecentHistogramIntervalsToTrack = 2;

    final long highestTrackableLatency;
    final int numberOfSignificantValueDigits;

    final int intervalEstimatorWindowLength;

    final long histogramUpdateInterval;
    final int numberOfRecentHistogramIntervalsToTrack;

    final PauseDetector pauseDetector;

    AtomicHistogram currentRecordingHistogram;

    Histogram currentPauseCorrectionHistogram;

    AtomicHistogram[] intervalRecordingHistograms;
    Histogram[] intervalPauseCorrectingHistograms;
    long[] intervalSampleTimes;

    volatile int latestIntervalHistogramIndex = 0;

    Histogram uncorrectedAccumulatedHistogram;
    Histogram accumulatedHistogram;

    private volatile long recordingStartEpoch = 0;
    private volatile long recordingEndEpoch = 0;
    static final AtomicLongFieldUpdater<LatencyStats> recordingStartEpochUpdater =
            AtomicLongFieldUpdater.newUpdater(LatencyStats.class, "recordingStartEpoch");
    static final AtomicLongFieldUpdater<LatencyStats> recordingEndEpochUpdater =
            AtomicLongFieldUpdater.newUpdater(LatencyStats.class, "recordingEndEpoch");

    static final Timer latencyStatsTasksTimer = new Timer();
    final PeriodicHistogramUpdateTask updateTask;
    final PauseTracker pauseTracker;

    long previousRecordingTime;
    final IntervalEstimator intervalEstimator;

    static PauseDetector defaultPauseDetector;

    static void setDefaultPauseDetector(PauseDetector pauseDetector) {
        defaultPauseDetector = pauseDetector;
    }

    public LatencyStats() {
        this(DEFAULT_HistogramUpdateInterval);
    }

    public LatencyStats(long histogramUpdateInterval) {
        this(defaultPauseDetector, histogramUpdateInterval);
    }

    public LatencyStats(PauseDetector pauseDetector, long histogramUpdateInterval) {
        this(DEFAULT_HighestTrackableLatency, DEFAULT_NumberOfSignificantValueDigits,
                histogramUpdateInterval, DEFAULT_numberOfRecentHistogramIntervalsToTrack,
                DEFAULT_IntervalEstimatorWindowLength, pauseDetector);
    }

    public LatencyStats(final long highestTrackableLatency, final int numberOfSignificantValueDigits,
                        final long histogramUpdateInterval, final int numberOfRecentHistogramIntervalsToTrack,
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
        this.histogramUpdateInterval = histogramUpdateInterval;
        this.numberOfRecentHistogramIntervalsToTrack = numberOfRecentHistogramIntervalsToTrack;
        this.intervalEstimatorWindowLength = intervalEstimatorWindowLength;

        // Create alternating recording histograms:
        currentRecordingHistogram = new AtomicHistogram(highestTrackableLatency, numberOfSignificantValueDigits);
        intervalRecordingHistograms = new AtomicHistogram[numberOfRecentHistogramIntervalsToTrack];
        for (int i = 0; i < numberOfRecentHistogramIntervalsToTrack; i++) {
            intervalRecordingHistograms[i] = new AtomicHistogram(highestTrackableLatency, numberOfSignificantValueDigits);
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

        intervalSampleTimes = new long[numberOfRecentHistogramIntervalsToTrack];

        // Create interval estimator:
        intervalEstimator = new TimeCappedMovingAverageIntervalEstimator(intervalEstimatorWindowLength, 5000000000L /* 5 sec */);

        // Create and schedule periodic update task:
        updateTask = new PeriodicHistogramUpdateTask(this.histogramUpdateInterval, this);

        // Create PauseTracker and register with pauseDetector:
        pauseTracker = new PauseTracker(pauseDetector, this);
    }

    public void recordLatency(long latency) {
        recordingStartEpochUpdater.incrementAndGet(this); // Used for otherwise un-synchronized histogram swapping
        trackRecordingInterval();
        currentRecordingHistogram.recordValue(latency);
        recordingEndEpochUpdater.incrementAndGet(this);

    }


    // Accumulated Histogram access:

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


    // Interval Histogram access:

    public synchronized Histogram getIntervalHistogram() {
        Histogram intervalHistogram = new Histogram(highestTrackableLatency, numberOfSignificantValueDigits);
        getIntervalHistogramInto(intervalHistogram);
        return intervalHistogram;
    }

    public synchronized void getIntervalHistogramInto(Histogram targetHistogram) {
        int index = latestIntervalHistogramIndex;
        intervalRecordingHistograms[index].copyInto(targetHistogram);
        targetHistogram.add(intervalPauseCorrectingHistograms[index]);
    }

    public synchronized void addIntervalHistogramTo(Histogram toHistogram) {
        int index = latestIntervalHistogramIndex;
        toHistogram.add(intervalRecordingHistograms[index]);
        toHistogram.add(intervalPauseCorrectingHistograms[index]);
    }

    public synchronized Histogram getUncorrectedIntervalHistogram() {
        int index = latestIntervalHistogramIndex;
        Histogram intervalHistogram = new Histogram(highestTrackableLatency, numberOfSignificantValueDigits);
        intervalRecordingHistograms[index].copyInto(intervalHistogram);
        return intervalHistogram;
    }

    public synchronized void forceIntervalUpdate() {
        updateHistograms();
    }

    synchronized void recordDetectedPause(long pauseLength, long pauseEndTime) {
        long estimatedInterval =  intervalEstimator.getEstimatedInterval(pauseEndTime - pauseLength);
        if (pauseLength > estimatedInterval) {
            currentPauseCorrectionHistogram.recordValueWithExpectedInterval(pauseLength, estimatedInterval);
        }
    }

    void trackRecordingInterval() {
        long now = System.nanoTime();
        long interval = now - previousRecordingTime;
        intervalEstimator.recordInterval(interval, now);
        previousRecordingTime = now;
    }

    void swapRecordingHistograms(int indexToSwap) {
        final AtomicHistogram tempHistogram = intervalRecordingHistograms[indexToSwap];
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
        intervalSampleTimes[indexToSwap] = System.nanoTime();

        // Update the latest interval histogram index.
        latestIntervalHistogramIndex = indexToSwap;

        // Make sure we are not in the middle of recording a value on the previously current recording histogram:
        long startEpoch = recordingStartEpochUpdater.get(this);
        while (recordingEndEpochUpdater.get(this) < startEpoch);

        uncorrectedAccumulatedHistogram.add(intervalRecordingHistograms[latestIntervalHistogramIndex]);
        accumulatedHistogram.add(intervalRecordingHistograms[latestIntervalHistogramIndex]);
        accumulatedHistogram.add(intervalPauseCorrectingHistograms[latestIntervalHistogramIndex]);
    }

    public void stop() {
        updateTask.stop();
        pauseTracker.stop();
    }

    /**
     * PauseTracker is used to feed pause correction histograms whenever a pause is reported:
     */
    static class PauseTracker extends WeakReference<LatencyStats> implements PauseDetectorListener {
        final PauseDetector pauseDetector;

        PauseTracker(final PauseDetector pauseDetector, final LatencyStats latencyStats) {
            super(latencyStats);
            this.pauseDetector = pauseDetector;
            // Register as listener:
            pauseDetector.addListener(this);
        }

        public void stop() {
            pauseDetector.removeListener(this);
        }

        public void handlePauseEvent(final long pauseLength, final long pauseEndTime) {
            final LatencyStats latencyStats = this.get();

            if (latencyStats != null) {
                latencyStats.recordDetectedPause(pauseLength, pauseEndTime);
            } else {
                // Remove listener:
                stop();
            }
        }
    }

    /**
     * PeriodicHistogramUpdateTask is used to collect interval and accumulated histograms with regular frequency:
     */
    static class PeriodicHistogramUpdateTask extends TimerTask {
        final WeakReference<LatencyStats> latencyStatsRef;

        PeriodicHistogramUpdateTask(final long histogramUpdateInterval, final LatencyStats latencyStats) {
            this.latencyStatsRef = new WeakReference<LatencyStats>(latencyStats);
            if (histogramUpdateInterval != 0) {
                latencyStatsTasksTimer.scheduleAtFixedRate(this, 0, (histogramUpdateInterval / 1000000L));
            }
        }

        public void stop() {
            this.cancel();
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
