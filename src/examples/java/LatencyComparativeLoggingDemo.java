/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.LatencyUtils.LatencyStats;
import org.LatencyUtils.SimplePauseDetector;

import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A simple demonstration of using a LatencyStats object to track and report on
 * latencies into a histogram log file. In normal, un-stalled execution, this
 * demo should result in simple expected behavior. When stalled manually (e.g.
 * by hitting ^Z, waiting a few seconds, and then continuing execution with fg),
 * latencyStats will detect the pause and compensate for it, which will be evident
 * in the reporting output.
 * <p>
 * This demo will output two log files: one for the "normal" pause-corrected output
 * (the one you should probably use by default), and one for the uncorrected output.
 * It demonstrates how the two result sets can be logged side by side, for later
 * comparison, e.g. for highlighting the effects of coordinated omission.
 * <p>
 * The histogram log outputs in this demo are in standard HdrHistogram histogram log
 * form. It can be post-processed by any tools that parse such logs, e.g. to extract
 * an accumulated histogram and it's overall percentile distribution for any time
 * range in the log, and/or to summarize percentile on an interval basis.
 * A good example tool for processing such log files can be found in jHiccup:
 * The jHiccup/jHiccupLogProcessor shell script should successfully process such files,
 * and the HistogramLogProcessor.java class in jHiccup or HdrHistogram should do the
 * same.
 */
public class LatencyComparativeLoggingDemo {
    static final long REPORTING_INTERVAL = 2 * 1000 * 1000 * 1000L; // report every 2 sec
    static final long RECORDING_INTERVAL = 5 * 1000 * 1000L; // record every 5 msec, @~200/sec
    static final long OPERATION_LATENCY = 1000 * 1000L; // 1 msec

    static final String defaultLogFileName = "latencyLoggingDemo.%date.%pid.hlog";
    static final String defaultUncorrectedLogFileName = "latencyLoggingDemoUncorrected.%date.%pid.hlog";
    static HistogramLogWriter histogramLogWriter;
    static HistogramLogWriter uncorrectedHistogramLogWriter;
    static long reportingStartTime;

    // Note that this will create and launch a default pause detector since one has not been set elsewhere:
    static final LatencyStats latencyStats = new LatencyStats();

    /**
     * Records an operation latency (with a fixed OPERATION_LATENCY value) for each call.
     */
    static class Recorder implements Runnable {
        @Override
        public void run() {
            // Record an "observed" latency for a fictitious operation:
            latencyStats.recordLatency(OPERATION_LATENCY);
        }
    }

    /**
     * Samples new interval data and reports on the observed interval histogram
     * and accumulated histogram.
     */
    static class Reporter implements Runnable {
        // We get initial histograms here and get *into* them later, at each sample point.
        //
        // This is an efficiency trick. While we could cleanly get a new histogram set at
        // each  interval sample, and do so with no instance variables to initialie here,
        // doing so would mean that a new set of histograms get constructed per sample.
        // Using the get...Into() variants at each sample, into previously allocated histograms
        // allows us to sample with no allocation.
        //
        // Getting the initial histograms with the regular get...() variant is a simple way of
        // initializing the sampling histograms without having to supply Histogram construction
        // parameters to match latencyStats settings..

        Histogram intervalHistogram = latencyStats.getIntervalHistogram();
        Histogram uncorrectedIntervalHistogram = latencyStats.getLatestUncorrectedIntervalHistogram();

        @Override
        public void run() {
            // Get the histogram (without allocating a new one each time):
            latencyStats.getIntervalHistogramInto(intervalHistogram);
            // Get the uncorrected interval histogram for the latest sampled interval:
            latencyStats.getLatestUncorrectedIntervalHistogramInto(uncorrectedIntervalHistogram);

            // Adjust start and end timestamps so they show offset from reportingStartTime
            // (by convention, they are set to indicate milliseconds form the epoch)
            intervalHistogram.setStartTimeStamp(intervalHistogram.getStartTimeStamp() - reportingStartTime);
            intervalHistogram.setEndTimeStamp(intervalHistogram.getEndTimeStamp() - reportingStartTime);

            uncorrectedIntervalHistogram.setStartTimeStamp(uncorrectedIntervalHistogram.getStartTimeStamp() - reportingStartTime);
            uncorrectedIntervalHistogram.setEndTimeStamp(uncorrectedIntervalHistogram.getEndTimeStamp() - reportingStartTime);

            // Report:
            histogramLogWriter.outputIntervalHistogram(intervalHistogram);
            uncorrectedHistogramLogWriter.outputIntervalHistogram(uncorrectedIntervalHistogram);
        }
    }

    static String fillInPidAndDate(String logFileName) {
        final String processName =
                java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        final String processID = processName.split("@")[0];
        final SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd.HHmm");
        final String formattedDate = formatter.format(new Date());

        logFileName = logFileName.replaceAll("%pid", processID);
        logFileName = logFileName.replaceAll("%date", formattedDate);
        return logFileName;
    }

    public static void main(final String[] args) throws FileNotFoundException {

        // Knowing that we're using a SimplePauseDetector, set it to verbose so that the user can see
        // the pause detection messages:
        ((SimplePauseDetector) latencyStats.getPauseDetector()).setVerbose(true);

        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2);

        // Record latencies on a "regular" basis. This will tend to record semi-reliably at
        // each interval as long as the JVM doesn't stall.
        executor.scheduleWithFixedDelay(new Recorder(), RECORDING_INTERVAL, RECORDING_INTERVAL, TimeUnit.NANOSECONDS);

        histogramLogWriter = new HistogramLogWriter(fillInPidAndDate(defaultLogFileName));
        histogramLogWriter.outputComment("[Logged with LatencyLoggingDemo]");
        histogramLogWriter.outputLogFormatVersion();

        uncorrectedHistogramLogWriter = new HistogramLogWriter(fillInPidAndDate(defaultUncorrectedLogFileName));
        uncorrectedHistogramLogWriter.outputComment("[Logged with LatencyLoggingDemo (Raw)]");
        uncorrectedHistogramLogWriter.outputLogFormatVersion();

        reportingStartTime = System.currentTimeMillis();
        // Force an interval sample right at the reporting start time (to start samples here):
        latencyStats.getIntervalHistogram();

        histogramLogWriter.outputStartTime(reportingStartTime);
        histogramLogWriter.outputLegend();

        uncorrectedHistogramLogWriter.outputStartTime(reportingStartTime);
        uncorrectedHistogramLogWriter.outputLegend();

        // Regularly report on observations into log file:
        executor.scheduleWithFixedDelay(new Reporter(), REPORTING_INTERVAL, REPORTING_INTERVAL, TimeUnit.NANOSECONDS);

        while (true);
    }
}
