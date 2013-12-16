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
import java.util.concurrent.TimeUnit;


/**
 * JUnit test for {@link org.LatencyUtils.SimplePauseDetector}
 */
public class LatencyStatsTest {

    static {
        System.setProperty("LatencyUtils.useActualTime", "false");
    }

    static final long highestTrackableValue = 3600L * 1000 * 1000 * 1000; // e.g. for 1 hr in nsec units
    static final int numberOfSignificantValueDigits = 2;

    @Test
    public void testLatencyStats() throws Exception {

        SimplePauseDetector pauseDetector = new SimplePauseDetector(1000000L /* 1 msec sleep */,
                10000000L /* 10 msec reporting threshold */, 3 /* thread count */, true /* verbose */);

        LatencyStats.setDefaultPauseDetector(pauseDetector);

        LatencyStats latencyStats = new LatencyStats();

        pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + 115000000L);

        TimeServices.moveTimeForward(5000L);
        TimeUnit.NANOSECONDS.sleep(1000000L); // Make sure things have some time to propagate
        TimeServices.moveTimeForward(1000000L);
        TimeUnit.NANOSECONDS.sleep(1000000L); // Make sure things have some time to propagate
        TimeServices.moveTimeForward(2000000L);
        TimeUnit.NANOSECONDS.sleep(1000000L); // Make sure things have some time to propagate
        TimeServices.moveTimeForward(110000000L);
        TimeUnit.NANOSECONDS.sleep(1000000L); // Make sure things have some time to propagate

        try {

            TimeUnit.NANOSECONDS.sleep(10000000L); // Make sure things have some time to propagate

            long startTime = TimeServices.nanoTime();

            long lastTime = startTime;
            for (int i = 0 ; i < 2000; i++) {
                pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (4 * 1000000L));
                TimeServices.moveTimeForwardMsec(5);
//                TimeUnit.NANOSECONDS.sleep(100000L); // Give things have some time to propagate
                long now = TimeServices.nanoTime();
                latencyStats.recordLatency(now - lastTime);
                lastTime = now;
            }

            TimeUnit.NANOSECONDS.sleep(100000L); // Make sure things have some time to propagate

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(lastTime));
            Histogram accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            Histogram intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            pauseDetector.stallDetectorThreads(0x7, 5000000000L);
            TimeUnit.NANOSECONDS.sleep(1000000L); // Make sure things have some time to propagate

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2000", 2000, accumulatedHistogram.getHistogramData().getTotalCount());

            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (500 * 1000000L));
            TimeServices.moveTimeForwardMsec(500);
            TimeUnit.NANOSECONDS.sleep(1000000L); // Make sure things have some time to propagate

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2000", 2000, accumulatedHistogram.getHistogramData().getTotalCount());

            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (500 * 1000000L));
            TimeServices.moveTimeForwardMsec(500);
            TimeUnit.NANOSECONDS.sleep(1000000L); // Make sure things have some time to propagate

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2999", 2999, accumulatedHistogram.getHistogramData().getTotalCount());

            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (2000 * 1000000L));
            TimeServices.moveTimeForwardMsec(2000);
            TimeUnit.NANOSECONDS.sleep(1000000L); // Make sure things have some time to propagate

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2999", 2999, accumulatedHistogram.getHistogramData().getTotalCount());

            for (int i = 0 ; i < 100; i++) {
                pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (5 * 1000000L));
                TimeServices.moveTimeForwardMsec(5);
                long now = TimeServices.nanoTime();
                latencyStats.recordLatency(now - lastTime);
                lastTime = now;
            }

            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (500 * 1000000L));
            TimeServices.moveTimeForwardMsec(500);
            TimeUnit.NANOSECONDS.sleep(1000000L); // Make sure things have some time to propagate

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();
            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());


            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (500 * 1000000L));
            TimeServices.moveTimeForwardMsec(500);
            TimeUnit.NANOSECONDS.sleep(1000000L); // Make sure things have some time to propagate

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();
            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2000", 3099, accumulatedHistogram.getHistogramData().getTotalCount());
        } catch (InterruptedException ex) {

        }

        latencyStats.stop();

        pauseDetector.shutdown();
    }

}

