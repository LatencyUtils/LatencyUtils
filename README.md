<a href="https://foojay.io/works-with-openjdk"><img align="right" src="https://github.com/foojayio/badges/raw/main/works_with_openjdk/Works-with-OpenJDK.png" width="100"></a>

LatencyUtils
============

A latency stats tracking package

The LatencyUtils package includes useful utilities for tracking latencies. Especially
in common in-process recording scenarios, which can exhibit significant coordinated
omission sensitivity without proper handling. LatencyStats instances are used to
track recorded latencies in the common use case the often follow this pattern:

```Java
 LatencyStats myOpStats = new LatencyStats();
 ...

 // During normal operation, record all operation latencies into a LatencyStats instance:

 long startTime = System.nanoTime();
 // Perform operation:
 doMyOperation(...);
 // Record operation latency:
 myOpStats.recordLatency(System.nanoTime() - startTime);
 ...

 // Later, report on stats collected:
 Histogram intervalHistogram = myOpStats.getIntervalHistogram();

 intervalHistogram.outputPercentileDistribution(System.out, 1000000.0);
```

The problem
-------------

Latency tracking of in-process operations usually consists simple time sampling around
a tracked operation. E.g. a database read operation for which latency statistics are
being developed may be surrounded by time measurement operation immediately before and
after the operation is performed, with the difference in time recorded in some aggregate
statistics gathering form (average, std. deviation, histogram,. etc.) which is later
used to report on the experienced latency behavior of the tracked operation.

The problem with this extremely common form of latency behavior tracking is that
whenever pauses occur in the system, latency statistics become significantly skewed
toward falsely-optimistic values. This occurs in two key ways:
- When a pause occurs during a tracked operation, a single long recorded latency
  will appear in the recorded values, with no long latencies associated with any
  pending requests that may be stalled by the pause.</li>
- When a pause occurs outside of the tracked operation (and outside of the tracked
  time window) no long latency value would be recorded, even though any requested
  operation would be stalled by the pause.</li>

The Solution
-------------

The LatencyStats class is designed for simple, drop-in use as a latency behavior
recording object in common in-process latency recording and tracking situations.
LatencyStats includes under-the-hood tracking and correction of pause effects,
compensating for coordinated omission. It does so by using pluggable pause detectors
and interval estimators that together with LatencyStats will transparently produce
corrected histogram values for the recorded latency behavior.
