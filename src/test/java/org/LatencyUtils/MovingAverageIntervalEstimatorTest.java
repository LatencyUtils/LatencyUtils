/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * JUnit test for {@link org.LatencyUtils.MovingAverageIntervalEstimatorTest}
 */
public class MovingAverageIntervalEstimatorTest {

    static {
        System.setProperty("LatencyUtils.useActualTime", "false");
    }

    static long detectedPauseLength = 0;

    @Test
    public void testMovingAverageIntervalEstimator() throws Exception {
        MovingAverageIntervalEstimator estimator = new MovingAverageIntervalEstimator(1024);

        long now = 0;

        for (int i = 0; i < 10000; i++) {
            now += 20;
            estimator.recordInterval(now);
        }

        Assert.assertEquals("expected interval to be 20", 20, estimator.getEstimatedInterval(now));


        for (int i = 0; i < 512; i++) {
            now += 40;
            estimator.recordInterval(now);
        }

        Assert.assertEquals("expected interval to be 20", 30, estimator.getEstimatedInterval(0));

        for (int i = 0; i < 256; i++) {
            now += 60;
            estimator.recordInterval(now);
        }

        Assert.assertEquals("expected interval to be 20", 40, estimator.getEstimatedInterval(0));
    }
}

