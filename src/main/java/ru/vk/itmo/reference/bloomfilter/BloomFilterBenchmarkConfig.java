package ru.vk.itmo.reference.bloomfilter;

/**
 * Нужен только для бенчмарков.
 */
public class BloomFilterBenchmarkConfig {
    public static boolean IS_ENABLED = false;
    public static int EXPECTED_INSERTIONS = 1_000;
    public static double FALSE_POSITIVE_RATE = 0.01;
}
