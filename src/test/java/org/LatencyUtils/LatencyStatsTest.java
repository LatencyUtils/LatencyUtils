/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import org.HdrHistogram.Histogram;
import org.junit.Assert;
import org.junit.Test;
import sun.nio.cs.HistoricallyNamedCharset;

import java.lang.ref.WeakReference;


/**
 * JUnit test for {@link org.LatencyUtils.SimplePauseDetector}
 */
public class LatencyStatsTest {

    static final long highestTrackableValue = 3600L * 1000 * 1000 * 1000; // e.g. for 1 hr in nsec units
    static final int numberOfSignificantValueDigits = 2;

    @Test
    public void testLatencyStats() throws Exception {
        SimplePauseDetector pauseDetector = new SimplePauseDetector(1000000L /* 1 msec sleep */,
                10000000L /* 10 msec reporting threshold */, 3 /* thread count */);

        LatencyStats.setDefaultPauseDetector(pauseDetector);

        LatencyStats latencyStats = new LatencyStats();

        try {

            Thread.sleep(50);

            long lastTime = System.nanoTime();
            for (int i = 0 ; i < 2000; i++) {
                Thread.sleep(5);
                long now = System.nanoTime();
                latencyStats.recordLatency(now - lastTime);
                lastTime = now;
            }

            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(lastTime));
            Histogram accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            Histogram intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            pauseDetector.stallDetectorThreads(0x7, 2000000000L);

            Thread.sleep(500);

            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(System.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            Thread.sleep(500);

            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(System.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());


            Thread.sleep(2000);

            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(System.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());


        } catch (InterruptedException ex) {

        }

        latencyStats.stop();

        pauseDetector.shutdown();
    }

}

