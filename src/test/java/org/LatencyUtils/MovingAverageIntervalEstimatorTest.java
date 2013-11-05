/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import org.junit.Assert;
import org.junit.Test;

import java.lang.ref.WeakReference;


/**
 * JUnit test for {@link org.LatencyUtils.SimplePauseDetector}
 */
public class MovingAverageIntervalEstimatorTest {

    static long detectedPauseLength = 0;

    @Test
    public void testMovingAverageIntervalEstimator() throws Exception {
        MovingAverageIntervalEstimator estimator = new MovingAverageIntervalEstimator(1024);

        long now = 0;

        for (int i = 0; i < 10000; i++) {
            now += i * 20;
            estimator.recordInterval(20, now);
        }

        Assert.assertEquals("expected interval to be 20", 20, estimator.getEstimatedInterval(0));


        for (int i = 0; i < 512; i++) {
            now += i * 40;
            estimator.recordInterval(40, now);
        }

        Assert.assertEquals("expected interval to be 20", 30, estimator.getEstimatedInterval(0));

        for (int i = 0; i < 256; i++) {
            now += i * 40;
            estimator.recordInterval(60, now);
        }

        Assert.assertEquals("expected interval to be 20", 40, estimator.getEstimatedInterval(0));
    }
}

