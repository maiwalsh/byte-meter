package com.rawbytes;

public class ByteMetrics {

    public long keyBytes;
    public long valueBytes;
    public long headerBytes;
    public long totalBytes;
    public long recordCount;

    public ByteMetrics() {
    }

    public ByteMetrics(long keyBytes, long valueBytes, long headerBytes, long recordCount) {
        this.keyBytes = keyBytes;
        this.valueBytes = valueBytes;
        this.headerBytes = headerBytes;
        this.totalBytes = keyBytes + valueBytes + headerBytes;
        this.recordCount = recordCount;
    }

    public void add(ByteMetrics other) {
        this.keyBytes += other.keyBytes;
        this.valueBytes += other.valueBytes;
        this.headerBytes += other.headerBytes;
        this.totalBytes += other.totalBytes;
        this.recordCount += other.recordCount;
    }
}
