package ru.vk.itmo;

import org.junit.jupiter.api.Timeout;
import ru.vk.itmo.reference.bloomfilter.BloomFilterBenchmarkConfig;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class BloomFilterBenchmarkTest extends BaseTest {
    // Вероятность того, что мы запросим значение, которого нет в SSTable.
    private static final double UNEXIST_ENTITY_GET_PROBABILITY = 0.5;
    private static final double FALSE_POSITIVE_RATE = 0.001;
    private static final int EXPECTED_INSERTIONS = 100_000;
    private static final int ENTRIES_PER_TABLE = 1_000;
    private static final int SSTABLES_NUMBER = 100;
    private static final boolean IS_ENABLED = true;

    private static final int BENCHMARK_ITERATIONS = 0;

    @DaoTest(stage = 6)
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    public void testBenchmarkBloomFilter(Dao<String, Entry<String>> dao) throws Exception {
        int entriesPerSSTable = ENTRIES_PER_TABLE;
        int sstables = SSTABLES_NUMBER;
        int totalEntries = entriesPerSSTable * sstables;
        Random rnd = new Random();

        setupBenchmark(IS_ENABLED, EXPECTED_INSERTIONS, FALSE_POSITIVE_RATE);
        populateDatabase(dao, totalEntries, entriesPerSSTable);
        double avgNanos = runBenchmark(dao, rnd, totalEntries);
        printResult(IS_ENABLED, EXPECTED_INSERTIONS, FALSE_POSITIVE_RATE, UNEXIST_ENTITY_GET_PROBABILITY, ENTRIES_PER_TABLE, sstables, avgNanos);
    }

    private double runBenchmark(Dao<String, Entry<String>> dao, Random random, int totalKeys) throws IOException {
        dao = DaoFactory.Factory.reopen(dao);
        
        var timings = new ArrayList<Long>();

        for (int round = 0; round < BENCHMARK_ITERATIONS; round++) {
            long elapsedNanos = measureRound(dao, random, totalKeys);
            timings.add(elapsedNanos);
        }

        return calculateAverage(timings);
    }

    private long measureRound(Dao<String, Entry<String>> dao, Random random, int count) {
        long roundDuration = 0;

        for (int idx = 0; idx < count; idx++) {
            boolean miss = random.nextDouble() < UNEXIST_ENTITY_GET_PROBABILITY;
            int id = miss ? generateNonExistingId(random, count) : generateExistingId(random, count);

            long started = System.nanoTime();
            Entry<String> result = dao.get(keyAt(id));
            long finished = System.nanoTime();

            roundDuration += (finished - started);

            validateResult(id, result);
        }

        return roundDuration;
    }

    private int generateNonExistingId(Random random, int bound) {
        return -(random.nextInt(bound) + 1);
    }

    private int generateExistingId(Random random, int bound) {
        return random.nextInt(bound);
    }

    private void validateResult(int id, Entry<String> result) {
        if (id >= 0) {
            assertSame(result, entryAt(id));
        } else {
            assertSame(result, null);
        }
    }

    private void populateDatabase(Dao<String, Entry<String>> dao, int total, int batchSize) throws IOException {
        for (int index = 0; index < total; index++) {
            dao.upsert(entry(keyAt(index), valueAt(index)));

            if (index > 0 && index % batchSize == 0) {
                dao.close();
                dao = DaoFactory.Factory.reopen(dao);
            }
        }
        dao.close();
    }

    private double calculateAverage(List<Long> samples) {
        return samples.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0);
    }


    private void setupBenchmark(boolean isEnabled, int expectedInsertions, double falsePositiveRate) {
        BloomFilterBenchmarkConfig.IS_ENABLED = isEnabled;
        BloomFilterBenchmarkConfig.EXPECTED_INSERTIONS = expectedInsertions;
        BloomFilterBenchmarkConfig.FALSE_POSITIVE_RATE = falsePositiveRate;
    }

    private void printResult(boolean bloomEnabled, int expectedInsertions, double fpRate, double unexistProbability, int entriesPerTable, int sstables, double avgNanos) {
        System.out.println("| Bloom Enabled | Filter Size | FP Rate | Unexist Probability | Expected Insertions | Entries per table | SSTables | AVG nanos per GET |");
        System.out.println("|---------------|-------------|---------|---------------------|---------------------|-------------------|----------|-------------------|");
        System.out.printf("| %-13s | %-11d | %-7.5f | %-19.2f | %-19d | %-17d | %-8d | %-17.2f |\n",
                bloomEnabled,
                expectedInsertions,
                fpRate,
                unexistProbability,
                expectedInsertions,
                entriesPerTable,
                sstables,
                avgNanos);
    }
}
