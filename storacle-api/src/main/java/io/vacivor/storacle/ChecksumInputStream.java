package io.vacivor.storacle;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class ChecksumInputStream extends FilterInputStream {
    private final Map<ChecksumAlgorithm, ChecksumAlgorithm.ChecksumAccumulator> accumulators;
    private Map<ChecksumAlgorithm, String> checksums;

    public ChecksumInputStream(InputStream input, Collection<ChecksumAlgorithm> algorithms) {
        super(Objects.requireNonNull(input, "input must not be null"));
        Objects.requireNonNull(algorithms, "algorithms must not be null");

        Set<ChecksumAlgorithm> uniqueAlgorithms = new LinkedHashSet<>(algorithms);
        if (uniqueAlgorithms.isEmpty()) {
            throw new IllegalArgumentException("algorithms must not be empty");
        }

        this.accumulators = new LinkedHashMap<>();
        for (ChecksumAlgorithm algorithm : uniqueAlgorithms) {
            if (algorithm == null) {
                throw new IllegalArgumentException("algorithms must not contain null");
            }
            this.accumulators.put(algorithm, algorithm.newAccumulator());
        }
    }

    @Override
    public int read() throws IOException {
        int value = super.read();
        if (value >= 0) {
            byte[] buffer = {(byte) value};
            updateAccumulators(buffer, 0, 1);
        }
        return value;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        int read = super.read(buffer, offset, length);
        if (read > 0) {
            updateAccumulators(buffer, offset, read);
        }
        return read;
    }

    public Map<ChecksumAlgorithm, String> checksums() {
        if (checksums != null) {
            return checksums;
        }
        Map<ChecksumAlgorithm, String> checksums = new LinkedHashMap<>();
        for (ChecksumAlgorithm.ChecksumAccumulator accumulator : accumulators.values()) {
            checksums.put(accumulator.algorithm(), accumulator.hex());
        }
        this.checksums = Map.copyOf(checksums);
        return this.checksums;
    }

    private void updateAccumulators(byte[] buffer, int offset, int length) {
        for (ChecksumAlgorithm.ChecksumAccumulator accumulator : accumulators.values()) {
            accumulator.update(buffer, offset, length);
        }
    }
}
