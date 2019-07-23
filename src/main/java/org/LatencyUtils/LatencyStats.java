/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.WriterReaderPhaser;

import java.lang.ref.WeakReference;

/**
 * LatencyStats objects are used to track and report on the behavior of latencies across measurements.
 * recorded into a a given LatencyStats instance. Latencies are recorded using
 * {@link #recordLatency}, which provides a thread safe, wait free, and lossless recording method.
 * The accumulated behavior across the recorded latencies in a given LatencyStats instance can be
 * examined in detail using interval and accumulated HdrHistogram histograms
 * (see {@link org.HdrHistogram.Histogram}).
 * <p>
 * LatencyStats instances maintain internal histogram data that track all recoded latencies. Interval
 * histogram data can be sampled with the {@link #getIntervalHistogram},
 * {@link #getIntervalHistogramInto}, or {@link #addIntervalHistogramTo} calls.
 * <p>
 * Recorded latencies are auto-corrected for experienced pauses by leveraging pause detectors and
 * moving window average interval estimators, compensating for coordinated omission. While typical
 * histogram use deals with corrected data, LatencyStats instances do keep track of the raw,
 * uncorrected records, which can be accessed via the {@link #getLatestUncorrectedIntervalHistogram}
 * and {@link #getLatestUncorrectedIntervalHistogramInto} calls.
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
    private static volatile PauseDetector defaultPauseDetector;

    // All times and time units are in nanoseconds

    private final long lowestTrackableLatency;
    private final long highestTrackableLatency;
    private final int numberOfSignificantValueDigits;

    private volatile AtomicHistogram activeRecordingHistogram;
    private Histogram activePauseCorrectionsHistogram;

    private AtomicHistogram inactiveRawDataHistogram;
    private Histogram inactivePauseCorrectionsHistogram;

    private final WriterReaderPhaser recordingPhaser = new WriterReaderPhaser();

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
     * Create a LatencyStats object with default settings.<br>
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
            // Lazy Initialization: Avoid double-checked locking race by using a local variable reading from a volatile:
            PauseDetector defDetector = defaultPauseDetector;
            if (defDetector == null) {
                // There is no pause detector supplied, and no default set. Set the default to a default
                // simple pause detector instance. [User feedback seems to be that this is preferable to
                // throwing an exception and forcing people to set the default themselves...]
                synchronized (LatencyStats.class) {
                    defDetector = defaultPauseDetector;
                    if (defDetector == null) {
                        defaultPauseDetector = defDetector = new SimplePauseDetector();
                    }
                }
            }
            this.pauseDetector = defDetector;
        } else {
            this.pauseDetector = pauseDetector;
        }

        this.lowestTrackableLatency = lowestTrackableLatency;
        this.highestTrackableLatency = highestTrackableLatency;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;

        // Create alternating recording histograms:
        activeRecordingHistogram = new AtomicHistogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        inactiveRawDataHistogram = new AtomicHistogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);

        // Create alternating pause correction histograms:
        activePauseCorrectionsHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        inactivePauseCorrectionsHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);

        // Create interval estimator:
        intervalEstimator = new TimeCappedMovingAverageIntervalEstimator(intervalEstimatorWindowLength,
                intervalEstimatorTimeCap, this.pauseDetector);

        // Create PauseTracker and register with pauseDetector:
        pauseTracker = new PauseTracker(this.pauseDetector, this);

        long now = System.currentTimeMillis();
        activeRecordingHistogram.setStartTimeStamp(now);
        activePauseCorrectionsHistogram.setStartTimeStamp(now);
    }

    /**
     * Record a latency value in the LatencyStats object
     * @param latency latency value (in nanoseconds) to record
     */
    public void recordLatency(long latency) {
        long criticalValueAtEnter = recordingPhaser.writerCriticalSectionEnter();

        try {
            trackRecordingInterval();
            activeRecordingHistogram.recordValue(latency);
        } finally {
            recordingPhaser.writerCriticalSectionExit(criticalValueAtEnter);
        }
    }


    // Interval Histogram access:

    /**
     * Get a new interval histogram which will include the value counts accumulated since the last
     * interval histogram was taken.
     * <p>
     * Calling {@link #getIntervalHistogram}() will reset
     * the interval value counts, and start accumulating value counts for the next interval.
     *
     * @return a copy of the latest interval latency histogram
     */
    public synchronized Histogram getIntervalHistogram() {
        Histogram intervalHistogram =
                new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
        getIntervalHistogramInto(intervalHistogram);
        return intervalHistogram;
    }

    /**
     * Place a copy of the value counts accumulated since the last interval histogram
     * was taken into {@code targetHistogram}.
     *
     * Calling {@link #getIntervalHistogramInto}() will reset
     * the interval value counts, and start accumulating value counts for the next interval.
     *
     * @param targetHistogram the histogram into which the interval histogram's data should be copied
     */
    public synchronized void getIntervalHistogramInto(Histogram targetHistogram) {
        try {
            recordingPhaser.readerLock();
            updateHistograms();
            inactiveRawDataHistogram.copyInto(targetHistogram);
            targetHistogram.add(inactivePauseCorrectionsHistogram);
        } finally {
            recordingPhaser.readerUnlock();
        }
    }

    /**
     * Add the values value counts accumulated since the last interval histogram was taken
     * into {@code toHistogram}.
     *
     * Calling {@link #addIntervalHistogramTo}() will reset
     * the interval value counts, and start accumulating value counts for the next interval.
     *
     * @param toHistogram the histogram into which the interval histogram's data should be added
     */
    public synchronized void addIntervalHistogramTo(Histogram toHistogram) {
        try {
            recordingPhaser.readerLock();
            updateHistograms();
            toHistogram.add(inactiveRawDataHistogram);
            toHistogram.add(inactivePauseCorrectionsHistogram);
        } finally {
            recordingPhaser.readerUnlock();
        }
    }

    /**
     * Get a copy of the uncorrected latest interval latency histogram. Values will not include
     * corrections for detected pauses. The interval histogram copies will include all values points
     * captured  up to the latest call to call to one of {@link #getIntervalHistogram},
     * {@link #getIntervalHistogramInto}, or {@link #addIntervalHistogramTo}.
     *
     * @return a copy of the latest uncorrected interval latency histogram
     */
    public synchronized Histogram getLatestUncorrectedIntervalHistogram() {
        try {
            recordingPhaser.readerLock();
            Histogram intervalHistogram = new Histogram(lowestTrackableLatency, highestTrackableLatency, numberOfSignificantValueDigits);
            getLatestUncorrectedIntervalHistogramInto(intervalHistogram);
            return intervalHistogram;
        } finally {
            recordingPhaser.readerUnlock();
        }
    }

    /**
     * Place a copy of the  values of the latest uncorrected interval latency histogram. Values will not include
     * corrections for detected pauses. The interval histogram copies will include all values points
     * captured  up to the latest call to call to one of {@link #getIntervalHistogram},
     * {@link #getIntervalHistogramInto}, or {@link #addIntervalHistogramTo}.
     *
     * @param targetHistogram the histogram into which the interval histogram's data should be copied
     */
    public synchronized void getLatestUncorrectedIntervalHistogramInto(Histogram targetHistogram) {
        try {
            recordingPhaser.readerLock();
            inactiveRawDataHistogram.copyInto(targetHistogram);
        } finally {
            recordingPhaser.readerUnlock();
        }
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
     * <br>Uses the following defaults:
     * <ul>
     * <li>lowestTrackableLatency:                  1000 (1 usec) </li>
     * <li>highestTrackableLatency:                 3600000000000L (1 hour) </li>
     * <li>numberOfSignificantValueDigits:          2 </li>
     * <li>intervalEstimatorWindowLength:           1024 </li>
     * <li>intervalEstimatorTimeCap:                10000000000L (10 sec) </li>
     * <li>pauseDetector:                           (use LatencyStats default) </li>
     * </ul>
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
        long criticalValueAtEnter = recordingPhaser.writerCriticalSectionEnter();
        try {
            long estimatedInterval = intervalEstimator.getEstimatedInterval(pauseEndTime);
            long observedLatencyMinbar = pauseLength - estimatedInterval;
            if (observedLatencyMinbar >= estimatedInterval) {
                activePauseCorrectionsHistogram.recordValueWithExpectedInterval(
                        observedLatencyMinbar,
                        estimatedInterval
                );
            }
        } finally {
            recordingPhaser.writerCriticalSectionExit(criticalValueAtEnter);
        }
    }

    private void trackRecordingInterval() {
        long now = TimeServices.nanoTime();
        intervalEstimator.recordInterval(now);
    }

    private void swapRecordingHistograms() {
        final AtomicHistogram tempHistogram = inactiveRawDataHistogram;
        inactiveRawDataHistogram = activeRecordingHistogram;
        activeRecordingHistogram = tempHistogram;
    }

    private void swapPauseCorrectionHistograms() {
        final Histogram tempHistogram = inactivePauseCorrectionsHistogram;
        inactivePauseCorrectionsHistogram = activePauseCorrectionsHistogram;
        activePauseCorrectionsHistogram = tempHistogram;
    }

    private synchronized void swapHistograms() {
        swapRecordingHistograms();
        swapPauseCorrectionHistograms();
    }

    private synchronized void updateHistograms() {
        try {
            recordingPhaser.readerLock();
            inactiveRawDataHistogram.reset();
            inactivePauseCorrectionsHistogram.reset();

            swapHistograms();
            long now = System.currentTimeMillis();
            activeRecordingHistogram.setStartTimeStamp(now);
            activePauseCorrectionsHistogram.setStartTimeStamp(now);
            inactiveRawDataHistogram.setEndTimeStamp(now);
            inactivePauseCorrectionsHistogram.setEndTimeStamp(now);

            // Make sure we are not in the middle of recording a value on the previously current recording histogram:

            // Flip phase on epochs to make sure no in-flight recordings are active on pre-flip phase:
            recordingPhaser.flipPhase();
        } finally {
            recordingPhaser.readerUnlock();
        }
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
