public class LossyCounters {
    private long frequency;
    private long maxError;

    LossyCounters(long frequency, long maxError) {
        this.frequency = frequency;
        this.maxError = maxError;
    }

    long getFrequency() {
        return frequency;
    }

    long getMaxError() {
        return maxError;
    }

    void setFrequency(long frequency) {
        this.frequency = frequency;
    }

    public void setMaxError(long maxError) {
        this.maxError = maxError;
    }
}
