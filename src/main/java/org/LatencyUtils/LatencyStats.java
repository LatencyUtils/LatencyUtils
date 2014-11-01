/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.AtomicHistogram;

import java.lang.ref.WeakReference;

/**
 * LatencyStats objects are used to track and report on the behavior of latencies across measurements.
 * recorded into a a given LatencyStats instance. Latencies are recorded using
 * {@link #recordLatency}, which provides a thread safe, wait free, and lossless recording method.
 * The accumulated behavior across the recorded latencies in a given LatencyStats instance can be
 * examined in detail using interval and accumulated HdrHistogram histograms
 * (see {@link org.HdrHistogram.Histogram}).
 * <p>
 * LatencyStats instances maintain internal histogram data that track all recoded latencies. All
 * histogram access forms return histogram data accumulated to the last {@link #forceIntervalSample}
 * call. As such, histograms data returned by the various get...() histogram access forms is always
 * stable and self-consistent (i.e. contents represent an atomic "instant in time", and do not appear
 * to change either during or after they are read).
 * <p>
 * Recorded latencies are auto-corrected for experienced pauses by leveraging pause detectors and
 * moving window average interval estimators, compensating for coordinated omission. While most
 * histogram access forms get...() and add...() operate with corrected data, LatencyStats
 * instances also keep track of the raw, uncorrected records, which can be accessed via the
 * getUncorrected...() histogram access forms.
 * <p>
 * LatencyStats objects can be instantiated either directly via the provided constructors, or by
 * using the fluent API builder supported by {@link org.LatencyUtils.LatencyStats.Builder}.
 *
 * <h3>Correction Technique</h3>
 * In addition to tracking the raw latency recordings provided via {@link #recordLatency}, each
 * LatencyStats instance maintains an internal interval estimator that tracks the expected
 * interval between latency recordings. Whenever a stall in measurement is detected by a given
 * pause detector, each LatencyStats instances that uses that pause detector will be notified,
 * and will generate correcting latency entries (that are separately tracked internally).
 * Correcting latency entries are computed to "fill in" detected measurement pauses by projecting
 * the observed recording rate into the pause gap, and creating a linearly diminishing latency
 * measurement for each missed recording interval.
 * <p>
 * Pause detection and interval estimation are both configurable, and each LatencyStats instance
 * can operate with potentially independent pause detector and interval estimator settings.
 * <p>
 * A configurable default pause detector is (by default) shared between LatencyStats instances
 * that are not provided with a specific pause detector at instantiation. If the default pause
 * detector is not explicitly set, it will itself default to creating (and starting) a single
 * instance of {@link org.LatencyUtils.SimplePauseDetector}, which uses consensus observation
 * of a pause across multiple observing threads as a detection technique.
 * <p>
 * Custom pause detectors can be provided (by subclassing {@link org.LatencyUtils.PauseDetector}).
 * E.g. a pause detector that pauses GC log output rather than directly measuring observations
 * can be constructed. A custom pause detector can be especially useful in situations where a
 * stall in the operation and latency measurement of an application's is known and detectable
 * by the application level, but would not be detectable as a process-wide stall in execution
 * (which {@link org.LatencyUtils.SimplePauseDetector} is built to detect).
 * <p>
 * Interval estimation is done by using a time-capped moving window average estimator, with
 * the expected interval computed to be the average of measurement intervals within the window
 * (with the window being capped by both count and time). See
 * {@link TimeCappedMovingAverageIntervalEstimator} for more details. The estimator window
 * length and time cap can both be configured when instantiating a LatencyStats object
 * (defaults are 1024, and 10 seconds).
 *
 */
public class LatencyStats {
    private static Builder defaultBuilder = new Builder();
    private static final TimeServices.ScheduledExecutor latencyStatsScheduledExecutor = new TimeServices.ScheduledExecutor();
    private static PauseDetector defaultPauseDetector;

    // All times and time units are in nanoseconds

    private final long lowestTrackableLatency;
    private final long highestTrackableLatency;
    private final int numberOfSignificantValueDigits;

    private volatile AtomicHistogram currentRecordingHistogram;
    private Histogram currentPauseCorrectionsHistogram;

    private AtomicHistogram intervalRawDataHistogram;
    private Histogram intervalPauseCorrectionsHistogram;

    private Histogram uncorrectedAccumulatedHistogram;
    private Histogram accumulatedHistogram;

    private final CriticalSectionPhaser recordingPhaser = new CriticalSectionPhaser();

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
     * Get the current default pause detector which will be used by newly constructed LatencyStats
     * instances when the constructor is not explicitly provided with one.
     * @return the current default pause detector
     */
    static public PauseDetector getDefaultPauseDetector() {
        return defaultPauseDetector;
    }

    /**
     * Create a LatencyStats object with default settings.<br></br>
     * <ul>
     * <li>use the default pause detector (supplied separately set using {@link #setDefaultPauseDetector}, which
     * will itself default to a {@link SimplePauseDetector} if not explicitly supplied)</li>
     * <li>use a default histogram update interval (1 sec)</li>
     * <li>use a default histogram range and accuracy (1 usec to 1hr, 2 decimal points of accuracy)</li>
     * <li>and use a default moving window estimator (1024 entry moving window, time capped at
     * 10 seconds)</li>
     * </ul>
     */
    public LatencyStats() {
        this(
                defaultBuilder.lowestTrackableLatency,
                defaultBuilder.highestTrackableLatency,
                defaultBuilder.numberOfSignificantValueDigits,
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
     * @param intervalEstimatorWindowLength Length of window in moving window interval estimator
     * @param intervalEstimatorTimeCap Time cap (in nanoseconds) of window in moving window interval estimator
     * @param pauseDetector The pause detector to use for identifying and correcting for pauses
     */
    public LatencyStats(final long lowestTrackableLatency,
                        final long highestTrackableLatency,
                        final int numberOfSignificantValueDigits,
                        final int intervalEstimatorWindowLength,
                        final long intervalEstimatorTimeCap,
                        final PauseDetector pauseDetector) {

        if (pauseDetector == null) {
            if (defaultPauseDetector == null) {
                // There is no pause detector supplied, and no default set. Set the default to a default
                // simple pause detector instance. [User feedback seems to be that this is preferrable to
                // throwing an exception and forcing people to set the default themselves...]
                synchronized (LatencyStats.class) {
                    if (defaultPauseDetector == null) {
                        defaultPauseDetector = new SimplePauseDetector();
                    }
                }
            }
            this.pauseDetector = defaultPauseDetector;
        } else {
            this.pauseDetector = pauseDetector;
        }

        this.lowestTrackableLatency = lowestTrackableLatency;
        this.highestTrackableLatency = highestTrackableLatency;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;

        // Create alternating recording histograms:
        currentRecordingHistogram = new AtomicHistogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        intervalRawDataHistogram = new AtomicHistogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);

        // Create alternating pause correction histograms:
        currentPauseCorrectionsHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        intervalPauseCorrectionsHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);

        // Create accumulated Histograms:
        accumulatedHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        uncorrectedAccumulatedHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);

        // Create interval estimator:
        intervalEstimator = new TimeCappedMovingAverageIntervalEstimator(intervalEstimatorWindowLength,
                intervalEstimatorTimeCap, this.pauseDetector);

        // Create PauseTracker and register with pauseDetector:
        pauseTracker = new PauseTracker(this.pauseDetector, this);

        long now = System.currentTimeMillis();
        currentRecordingHistogram.setStartTimeStamp(now);
        currentPauseCorrectionsHistogram.setStartTimeStamp(now);
    }

    /**
     * Record a latency value in the LatencyStats object
     * @param latency latency value (in nanoseconds) to record
     */
    public void recordLatency(long latency) {
        long criticalValueAtEnter = recordingPhaser.enteringCriticalSection();

        try {
            trackRecordingInterval();
            currentRecordingHistogram.recordValue(latency);
        } finally {
            recordingPhaser.doneWithCriticalSection(criticalValueAtEnter);
        }
    }


    // Accumulated Histogram access:

    /**
     * Get a copy of the latest accumulated latency histogram (the one sampled at the last
     * call to {@link #forceIntervalSample}).
     * @return a copy of the latest accumulated latency histogram
     */
    public synchronized Histogram getAccumulatedHistogram() {
        return accumulatedHistogram.copy();
    }

    /**
     * Place a copy of the values of the latest accumulated latency histogram (the one sampled at the last
     * call to {@link #forceIntervalSample}) into the given histogram
     * @param targetHistogram the histogram into which the accumulated histogram's data should be copied
     */
    public synchronized void getAccumulatedHistogramInto(Histogram targetHistogram) {
        accumulatedHistogram.copyInto(targetHistogram);
    }

    /**
     * Add the values of the latest accumulated latency histogram (the one sampled at the last
     * call to {@link #forceIntervalSample}) into the given histogram
     * @param toHistogram the histogram into which the accumulated histogram's data should be added
     */
    public synchronized void addAccumulatedHistogramTo(Histogram toHistogram) {
        toHistogram.add(accumulatedHistogram);
    }

    /**
     * Get a copy of the uncorrected accumulated latency histogram (the one sampled at the last
     * call to {@link #forceIntervalSample}). Values will not include corrections
     * for detected pauses.
     * @return a copy of the latest uncorrected accumulated latency histogram
     */
    public synchronized Histogram getUncorrectedAccumulatedHistogram() {
        return uncorrectedAccumulatedHistogram.copy();
    }


    // Interval Histogram access:

    /**
     * Get a copy of the latest interval latency histogram (the one sampled at the last
     * call to {@link #forceIntervalSample}):
     * @return a copy of the latest interval latency histogram
     */
    public synchronized Histogram getIntervalHistogram() {
        Histogram intervalHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        getIntervalHistogramInto(intervalHistogram);
        return intervalHistogram;
    }

    /**
     * Place a copy of the  values of the latest interval latency histogram (the one sampled at the last
     * call to {@link #forceIntervalSample}) into the given histogram
     * @param targetHistogram the histogram into which the interval histogram's data should be copied
     */
    public synchronized void getIntervalHistogramInto(Histogram targetHistogram) {
        intervalRawDataHistogram.copyInto(targetHistogram);
        targetHistogram.add(intervalPauseCorrectionsHistogram);
    }

    /**
     * Add the values of the latest interval latency histogram (the one sampled at the last
     * call to {@link #forceIntervalSample}) into the given histogram
     * @param toHistogram the histogram into which the interval histogram's data should be added
     */
    public synchronized void addIntervalHistogramTo(Histogram toHistogram) {
        toHistogram.add(intervalRawDataHistogram);
        toHistogram.add(intervalPauseCorrectionsHistogram);
    }

    /**
     * Get a copy of the uncorrected latest interval latency histogram (the one sampled at the last
     * call to {@link #forceIntervalSample}). Values will not include corrections
     * for detected pauses.
     * @return a copy of the latest uncorrected interval latency histogram
     */
    public synchronized Histogram getUncorrectedIntervalHistogram() {
        Histogram intervalHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        getUncorrectedIntervalHistogramInto(intervalHistogram);
        return intervalHistogram;
    }

    /**
     * Place a copy of the  values of the latest uncorrected interval latency histogram (the one
     * sampled at the last call to {@link #forceIntervalSample}) into the given histogram
     * @param targetHistogram the histogram into which the interval histogram's data should be copied
     */
    public synchronized void getUncorrectedIntervalHistogramInto(Histogram targetHistogram) {
        intervalRawDataHistogram.copyInto(targetHistogram);
    }


    /**
     * Force an update of the interval and accumulated histograms data from the current recorded data.
     * Note that the interval and accumulated histograms observed with the various get...() calls are
     * ONLY updated when {@link #forceIntervalSample} is called.
     */
    public synchronized void forceIntervalSample() {
        updateHistograms();
    }

    /**
     * Reset the contents of the accumulated histogram
     */
    public synchronized void resetAccumulatedHistogram() {
        long now = System.currentTimeMillis();
        accumulatedHistogram.reset();
        accumulatedHistogram.setStartTimeStamp(now);
        uncorrectedAccumulatedHistogram.reset();
        uncorrectedAccumulatedHistogram.setStartTimeStamp(now);
    }

    /**
     * Stop operation of this LatencyStats object, removing it from the pause detector's notification list
     */
    public synchronized void stop() {
        pauseTracker.stop();
        latencyStatsScheduledExecutor.shutdown();
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
     * <li>intervalEstimatorWindowLength:           1024</li>
     * <li>intervalEstimatorTimeCap:                10000000000L (10 sec)</li>
     * <li>pauseDetector:                           (use LatencyStats default)</li>
     */
    public static class Builder {
        private long lowestTrackableLatency = 1000L; /* 1 usec */
        private long highestTrackableLatency = 3600000000000L; /* 1 hr */
        private int numberOfSignificantValueDigits = 2;
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
                    intervalEstimatorWindowLength,
                    intervalEstimatorTimeCap,
                    pauseDetector);
        }
    }

    private synchronized void recordDetectedPause(long pauseLength, long pauseEndTime) {
        long estimatedInterval =  intervalEstimator.getEstimatedInterval(pauseEndTime);
        long observedLatencyMinbar = pauseLength - estimatedInterval;
        if (observedLatencyMinbar >= estimatedInterval) {
            currentPauseCorrectionsHistogram.recordValueWithExpectedInterval(observedLatencyMinbar, estimatedInterval);
        }
    }

    private void trackRecordingInterval() {
        long now = TimeServices.nanoTime();
        intervalEstimator.recordInterval(now);
    }

    private void swapRecordingHistograms() {
        final AtomicHistogram tempHistogram = intervalRawDataHistogram;
        intervalRawDataHistogram = currentRecordingHistogram;
        currentRecordingHistogram = tempHistogram;
    }

    private void swapPauseCorrectionHistograms() {
        final Histogram tempHistogram = intervalPauseCorrectionsHistogram;
        intervalPauseCorrectionsHistogram = currentPauseCorrectionsHistogram;
        currentPauseCorrectionsHistogram = tempHistogram;
    }

    private synchronized void swapHistograms() {
        swapRecordingHistograms();
        swapPauseCorrectionHistograms();
    }

    private synchronized void updateHistograms() {
        intervalRawDataHistogram.reset();
        intervalPauseCorrectionsHistogram.reset();

        swapHistograms();
        long now = System.currentTimeMillis();
        currentRecordingHistogram.setStartTimeStamp(now);
        currentPauseCorrectionsHistogram.setStartTimeStamp(now);
        intervalRawDataHistogram.setEndTimeStamp(now);
        intervalPauseCorrectionsHistogram.setEndTimeStamp(now);

        // Make sure we are not in the middle of recording a value on the previously current recording histogram:

        // Flip phase on epochs to make sure no in-flight recordings are active on pre-flip phase:
        recordingPhaser.flipPhase();

        uncorrectedAccumulatedHistogram.add(intervalRawDataHistogram);
        uncorrectedAccumulatedHistogram.setEndTimeStamp(now);

        accumulatedHistogram.add(intervalRawDataHistogram);
        accumulatedHistogram.add(intervalPauseCorrectionsHistogram);
        accumulatedHistogram.setEndTimeStamp(now);
    }

    /**
     * PauseTracker is used to feed pause correction histograms whenever a pause is reported:
     */
    private static class PauseTracker extends WeakReference<LatencyStats> implements PauseDetectorListener {
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
}
