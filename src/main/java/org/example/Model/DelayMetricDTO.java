package org.example.Model;

public class DelayMetricDTO {
    public long maxDelay;
    public long sum;
    public long count;

    public double getAvgDelay() {
        return count == 0 ? 0 : (double)sum / count;
    }
}
