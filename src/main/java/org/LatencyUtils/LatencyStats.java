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

/**
 * LatencyStats objects are used to track the behavior of latencies recorded using {@link #recordLatency}).
 * Latency behavior can be examined using detailed interval and accumulated histograms
 * (see {@link org.HdrHistogram.Histogram}).
 * Recorded latencies are auto-corrected for experienced pauses by leveraging pause detectors and moving window
 * average interval estimators, compensating for coordinated omission.
 * <p>
 * <h3>Correction Technique</h3>
 * Whenever a pause is detected, appropriate pause correction values are captured in each LatencyStats instance,
 * based on that instance's estimated inter-recording intervalEndTimes.
 */
public class LatencyStats {
    private static PauseDetector defaultPauseDetector;
    private static Builder defaultBuilder = new Builder();

    // All times and time units are in nanoseconds

    private final long lowestTrackableLatency;
    private final long highestTrackableLatency;
    private final int numberOfSignificantValueDigits;

    private final long histogramUpdateInterval;
    private final int numberOfRecentHistogramIntervalsToTrack;

    private AtomicHistogram currentRecordingHistogram;
    private Histogram currentPauseCorrectionHistogram;

    private AtomicHistogram[] intervalRecordingHistograms;
    private Histogram[] intervalPauseCorrectingHistograms;
    private long[] intervalCaptureTimes;

    private volatile int latestIntervalHistogramIndex = 0;

    private Histogram uncorrectedAccumulatedHistogram;
    private Histogram accumulatedHistogram;

    private volatile long recordingStartEpoch = 0;
    private volatile long recordingEndEpoch = 0;
    private static final AtomicLongFieldUpdater<LatencyStats> recordingStartEpochUpdater =
            AtomicLongFieldUpdater.newUpdater(LatencyStats.class, "recordingStartEpoch");
    private static final AtomicLongFieldUpdater<LatencyStats> recordingEndEpochUpdater =
            AtomicLongFieldUpdater.newUpdater(LatencyStats.class, "recordingEndEpoch");

    private static final Timer latencyStatsTasksTimer = new Timer();
    private final PeriodicHistogramUpdateTask updateTask;
    private final PauseTracker pauseTracker;

    private final IntervalEstimator intervalEstimator;

    private final PauseDetector pauseDetector;


    /**
     * Set the default pause detector for the LatencyStats class. Used by constructors that do
     * not explicitly provide a pause detector.
     *
     * @param pauseDetector the pause detector to use as a default when no explicit pause detector is provided
     */
    static public void setDefaultPauseDetector(PauseDetector pauseDetector) {
        defaultPauseDetector = pauseDetector;
    }

    /**
     * Create a LatencyStats object with default settings:
     * use the default pause detector (must be separately set using {@link #setDefaultPauseDetector}), a default
     * histogram update interval (1 sec), a default histogram range and accuracy (1 usec to 1hr,
     * 2 decimal points of accuracy), and a default moving window estimator (1024 entry moving window, time capped at
     * 10 seconds).
     */
    public LatencyStats() {
        this(
                defaultBuilder.lowestTrackableLatency,
                defaultBuilder.highestTrackableLatency,
                defaultBuilder.numberOfSignificantValueDigits,
                defaultBuilder.histogramUpdateInterval,
                defaultBuilder.numberOfRecentHistogramIntervalsToTrack,
                defaultBuilder.intervalEstimatorWindowLength,
                defaultBuilder.intervalEstimatorTimeCap,
                defaultBuilder.pauseDetector
        );
    }

    /**
     *
     * @param lowestTrackableLatency   Lowest trackable latency in latency histograms
     * @param highestTrackableLatency   Highest trackable latency in latency histograms
     * @param numberOfSignificantValueDigits Number of significant [decimal] digits of accuracy in latency histograms
     * @param histogramUpdateInterval   The length (in nanoseconds) of the automatically updating time interval.
     * @param numberOfRecentHistogramIntervalsToTrack Number of recent intervalEndTimes to track
     * @param intervalEstimatorWindowLength Length of window in moving window interval estimator
     * @param intervalEstimatorTimeCap Time cap (in nanoseconds) of window in moving window interval estimator
     * @param pauseDetector The pause detector to use for identifying and correcting for pauses
     */
    public LatencyStats(final long lowestTrackableLatency,
                        final long highestTrackableLatency,
                        final int numberOfSignificantValueDigits,
                        final long histogramUpdateInterval,
                        final int numberOfRecentHistogramIntervalsToTrack,
                        final int intervalEstimatorWindowLength,
                        final long intervalEstimatorTimeCap,
                        final PauseDetector pauseDetector) {

        if (pauseDetector == null) {
            if (defaultPauseDetector == null) {
                throw new IllegalStateException("If a pause detector is not supplied, a default pause detector must first be set for LatencyStats.");
            }
            this.pauseDetector = defaultPauseDetector;
        } else {
            this.pauseDetector = pauseDetector;
        }

        this.lowestTrackableLatency = lowestTrackableLatency;
        this.highestTrackableLatency = highestTrackableLatency;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        this.histogramUpdateInterval = histogramUpdateInterval;
        this.numberOfRecentHistogramIntervalsToTrack = numberOfRecentHistogramIntervalsToTrack;

        // Create alternating recording histograms:
        currentRecordingHistogram = new AtomicHistogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        intervalRecordingHistograms = new AtomicHistogram[numberOfRecentHistogramIntervalsToTrack];
        for (int i = 0; i < numberOfRecentHistogramIntervalsToTrack; i++) {
            intervalRecordingHistograms[i] = new AtomicHistogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        }

        // Create alternating pause correction histograms:
        currentPauseCorrectionHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        intervalPauseCorrectingHistograms = new Histogram[numberOfRecentHistogramIntervalsToTrack];
        for (int i = 0; i < numberOfRecentHistogramIntervalsToTrack; i++) {
            intervalPauseCorrectingHistograms[i] = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        }
        // Create accumulated Histograms:
        accumulatedHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        uncorrectedAccumulatedHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);

        intervalCaptureTimes = new long[numberOfRecentHistogramIntervalsToTrack];

        // Create interval estimator:
        intervalEstimator = new TimeCappedMovingAverageIntervalEstimator(intervalEstimatorWindowLength,
                intervalEstimatorTimeCap, this.pauseDetector);

        // Create and schedule periodic update task:
        updateTask = new PeriodicHistogramUpdateTask(this.histogramUpdateInterval, this);

        // Create PauseTracker and register with pauseDetector:
        pauseTracker = new PauseTracker(this.pauseDetector, this);
    }

    /**
     * Record a latency value in the LatencyStats object
     * @param latency latency value (in nanoseconds) to record
     */
    public void recordLatency(long latency) {
        recordingStartEpochUpdater.incrementAndGet(this); // Used for otherwise un-synchronized histogram swapping
        trackRecordingInterval();
        currentRecordingHistogram.recordValue(latency);
        recordingEndEpochUpdater.incrementAndGet(this);
    }


    // Accumulated Histogram access:

    /**
     * Get a copy of the accumulated latency histogram:
     * @return a copy of the accumulated latency histogram
     */
    public synchronized Histogram getAccumulatedHistogram() {
        return accumulatedHistogram.copy();
    }

    /**
     * Place a copy of the accumulated latency histogram's values into the given histogram
     * @param targetHistogram the histogram into which the accumulated histogram's data should be copied
     */
    public synchronized void getAccumulatedHistogramInto(Histogram targetHistogram) {
        accumulatedHistogram.copyInto(targetHistogram);
    }

    /**
     * Add the values of the accumulated latency histogram to the given histogram
     * @param toHistogram the histogram into which the accumulated histogram's data should be added
     */
    public synchronized void addAccumulatedHistogramTo(Histogram toHistogram) {
        toHistogram.add(accumulatedHistogram);
    }

    /**
     * Get a copy of the uncorrected accumulated latency histogram (values will not include corrections
     * for detected pauses)
     * @return a copy of the uncorrected accumulated latency histogram
     */
    public synchronized Histogram getUncorrectedAccumulatedHistogram() {
        return uncorrectedAccumulatedHistogram.copy();
    }


    // Interval Histogram access:

    /**
     * Get a copy of the latest interval latency histogram:
     * @return a copy of the interval latency histogram
     */
    public synchronized Histogram getIntervalHistogram() {
        Histogram intervalHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        getIntervalHistogramInto(intervalHistogram);
        return intervalHistogram;
    }

    /**
     * Place a copy of the latest interval latency histogram's values into the given histogram
     * @param targetHistogram the histogram into which the interval histogram's data should be copied
     */
    public synchronized void getIntervalHistogramInto(Histogram targetHistogram) {
        int index = latestIntervalHistogramIndex;
        intervalRecordingHistograms[index].copyInto(targetHistogram);
        targetHistogram.add(intervalPauseCorrectingHistograms[index]);
    }

    /**
     * Add the values of the latest interval latency histogram to the given histogram
     * @param toHistogram the histogram into which the interval histogram's data should be added
     */
    public synchronized void addIntervalHistogramTo(Histogram toHistogram) {
        int index = latestIntervalHistogramIndex;
        toHistogram.add(intervalRecordingHistograms[index]);
        toHistogram.add(intervalPauseCorrectingHistograms[index]);
    }

    /**
     * Get a copy of the uncorrected latest interval latency histogram (values will not include corrections
     * for detected pauses)
     * @return a copy of the uncorrected interval latency histogram
     */
    public synchronized Histogram getUncorrectedIntervalHistogram() {
        int index = latestIntervalHistogramIndex;
        Histogram intervalHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        intervalRecordingHistograms[index].copyInto(intervalHistogram);
        return intervalHistogram;
    }

    /**
     * Force an update of the interval and accumulated histograms from the current recorded data. A new
     * interval sample will be performed regardless of the timing of histogramUpdateInterval set at construction
     * time. When histogramUpdateInterval is set to 0, {@link #forceIntervalUpdate} provides the only means
     * by which intervalEndTimes are updated.
     */
    public synchronized void forceIntervalUpdate() {
        updateHistograms();
    }

    /**
     * Reset the contents of the accumulated histogram
     */
    public synchronized void resetAccumulatedHistogram() {
        accumulatedHistogram.reset();
        uncorrectedAccumulatedHistogram.reset();
    }

    /**
     * Stop operation of this LatencyStats object, removing it from the pause detector's notification list
     */
    public synchronized void stop() {
        updateTask.stop();
        pauseTracker.stop();
    }

    /**
     * get the IntervalEstimator used by this LatencyStats object
     * @return the IntervalEstimator used by this LatencyStats object
     */
    public IntervalEstimator getIntervalEstimator() {
        return intervalEstimator;
    }

    /**
     * get the PauseDetector used by this LatencyStats object
     * @return the PauseDetector used by this LatencyStats object
     */
    public PauseDetector getPauseDetector() {
        return pauseDetector;
    }

    /**
     * A fluent API builder class for creating LatencyStats objects.
     * <br>Uses the following defaults:</br>
     * <li>lowestTrackableLatency:                  1000 (1 usec)</li>
     * <li>highestTrackableLatency:                 3600000000000L (1 hour)</li>
     * <li>numberOfSignificantValueDigits:          2</li>
     * <li>histogramUpdateInterval:                 1000000000L (1 sec) </li>
     * <li>numberOfRecentHistogramIntervalsToTrack: 2</li>
     * <li>intervalEstimatorWindowLength:           1024</li>
     * <li>intervalEstimatorTimeCap:                10000000000L (10 sec)</li>
     * <li>pauseDetector:                           (use LatencyStats default)</li>
     */
    public static class Builder {
        private long lowestTrackableLatency = 1000L; /* 1 usec */
        private long highestTrackableLatency = 3600000000000L; /* 1 hr */
        private int numberOfSignificantValueDigits = 2;
        private long histogramUpdateInterval = 1000000000L; /* 1 sec */
        private int numberOfRecentHistogramIntervalsToTrack = 2;
        private int intervalEstimatorWindowLength = 1024;
        private long intervalEstimatorTimeCap = 10000000000L; /* 10 sec */
        private PauseDetector pauseDetector = null;

        public static Builder create() {
            return new Builder();
        }

        public Builder() {

        }

        public Builder lowestTrackableLatency(long lowestTrackableLatency) {
            this.lowestTrackableLatency = lowestTrackableLatency;
            return this;
        }


        public Builder highestTrackableLatency(long highestTrackableLatency) {
            this.highestTrackableLatency = highestTrackableLatency;
            return this;
        }


        public Builder numberOfSignificantValueDigits(int numberOfSignificantValueDigits) {
            this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
            return this;
        }


        public Builder histogramUpdateInterval(long histogramUpdateInterval) {
            this.histogramUpdateInterval = histogramUpdateInterval;
            return this;
        }


        public Builder numberOfRecentHistogramIntervalsToTrack(int numberOfRecentHistogramIntervalsToTrack) {
            this.numberOfRecentHistogramIntervalsToTrack = numberOfRecentHistogramIntervalsToTrack;
            return this;
        }


        public Builder intervalEstimatorWindowLength(int intervalEstimatorWindowLength) {
            this.intervalEstimatorWindowLength = intervalEstimatorWindowLength;
            return this;
        }


        public Builder intervalEstimatorTimeCap(long intervalEstimatorTimeCap) {
            this.intervalEstimatorTimeCap = intervalEstimatorTimeCap;
            return this;
        }


        public Builder pauseDetector(PauseDetector pauseDetector) {
            this.pauseDetector = pauseDetector;
            return this;
        }


        public LatencyStats build() {
            return new LatencyStats(lowestTrackableLatency,
                    highestTrackableLatency,
                    numberOfSignificantValueDigits,
                    histogramUpdateInterval,
                    numberOfRecentHistogramIntervalsToTrack,
                    intervalEstimatorWindowLength,
                    intervalEstimatorTimeCap,
                    pauseDetector);
        }
    }

    synchronized void recordDetectedPause(long pauseLength, long pauseEndTime) {
        long estimatedInterval =  intervalEstimator.getEstimatedInterval(pauseEndTime - pauseLength);
        if (pauseLength > estimatedInterval) {
            currentPauseCorrectionHistogram.recordValueWithExpectedInterval(pauseLength, estimatedInterval);
        }
    }

    void trackRecordingInterval() {
        long now = System.nanoTime();
        intervalEstimator.recordInterval(now);
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
        intervalCaptureTimes[indexToSwap] = System.nanoTime();

        // Update the latest interval histogram index.
        latestIntervalHistogramIndex = indexToSwap;

        // Make sure we are not in the middle of recording a value on the previously current recording histogram:
        long startEpoch = recordingStartEpochUpdater.get(this);
        while (recordingEndEpochUpdater.get(this) < startEpoch);

        uncorrectedAccumulatedHistogram.add(intervalRecordingHistograms[latestIntervalHistogramIndex]);
        accumulatedHistogram.add(intervalRecordingHistograms[latestIntervalHistogramIndex]);
        accumulatedHistogram.add(intervalPauseCorrectingHistograms[latestIntervalHistogramIndex]);
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
