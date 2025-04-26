package ru.vk.itmo.reference.bloomfilter;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.BitSet;

/**
 * Реализация Bloom-фильтра.
 */
public class BloomFilter {
    private final int m; // Размер битового массива
    private final int k; // Количество хеш-функций
    private final BitSet bitSet; // Битовый массив для хранения состояния фильтра

    /**
     * Создает новый Bloom-фильтр с параметрами по умолчанию.
     *
     * @return экземпляр {@link BloomFilter}
     */
    public static BloomFilter newFilter() {
        return new BloomFilter(BloomFilterBenchmarkConfig.EXPECTED_INSERTIONS, BloomFilterBenchmarkConfig.FALSE_POSITIVE_RATE);
    }

    /**
     * Конструктор Bloom-фильтра.
     *
     * @param expectedInsertions ожидаемое количество элементов
     * @param falsePositiveRate  требуемая вероятность ложноположительного срабатывания
     */
    public BloomFilter(int expectedInsertions, double falsePositiveRate) {
        int calculatedM = (int) Math.ceil(-expectedInsertions * Math.log(falsePositiveRate) / Math.pow(Math.log(2), 2));
        // Округляем вверх до ближайшего кратного 64, чтобы при записи на диск минимизировать потери из-за неиспользуемых битов.
        this.m = ((calculatedM + 63) / 64) * 64;
        this.k = (int) Math.ceil((double) m / expectedInsertions * Math.log(2));
        this.bitSet = new BitSet(m);
    }

    /**
     * Добавляет элемент в Bloom-фильтр.
     *
     * @param key элемент, который необходимо добавить
     */
    public void add(MemorySegment key) {
//        byte[] keyBytes = key.toArray(ValueLayout.JAVA_BYTE);
        for (int i = 0; i < k; i++) {
            int hash = BloomFilterUtils.hashCode(key, (int) key.byteSize(), i);
            int index = Math.abs(hash % m);
            bitSet.set(index);
        }
    }

    /**
     * Возвращает битовый массив фильтра.
     *
     * @return {@link BitSet}, представляющий состояние фильтра
     */
    public BitSet getBitSet() {
        return bitSet;
    }

    /**
     * Возвращает размер битового массива (m).
     *
     * @return размер фильтра в битах
     */
    public int getSize() {
        return m;
    }

    /**
     * Возвращает количество хеш-функций (k).
     *
     * @return количество хеш-функций
     */
    public int getHashCount() {
        return k;
    }
}
