package ru.vk.itmo.reference.bloomfilter;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.BitSet;

/**
 * Утилитарные методы для работы с Bloom-фильтрами.
 */
public class BloomFilterUtils {

    /**
     * Преобразует {@link BitSet} в массив {@code long[]} для записи в файл.
     * <p>
     * Размер {@code BitSet} в используемом Bloom-фильтре всегда кратен 64.
     *
     * @param bitSet битовый массив, представляющий фильтр
     * @return массив {@code long[]} для компактного хранения битов
     */
    public static long[] toLongArray(BitSet bitSet) {
        long[] result = new long[bitSet.size() / 64];
        for (int i = 0; i < result.length; i++) {
            long value = 0L;
            for (int j = 0; j < 64; j++) {
                int bitIndex = i * 64 + j;
                if (bitIndex >= bitSet.length()) {
                    break;
                }
                if (bitSet.get(bitIndex)) {
                    value |= 1L << j;
                }
            }
            result[i] = value;
        }
        return result;
    }

    /**
     * Проверяет наличие элемента в Bloom-фильтре.
     *
     * @param k     количество хеш-функций
     * @param longs массив {@code long[]} битового представления фильтра
     * @param key   ключ для проверки
     * @return {@code true}, если элемент может присутствовать в фильтре;
     *         {@code false}, если элемент точно отсутствует
     */
    public static boolean contains(long k, long[] longs, MemorySegment key) {
//        byte[] keyBytes = key.asByteBuffer().array();
        int m = longs.length * Long.SIZE; // Общее количество битов

        for (int i = 0; i < k; i++) {
            int hash = hashCode(key, (int)key.byteSize(), i);
            int bitIndex = Math.abs(hash % m);

            int longIndex = bitIndex / Long.SIZE;
            int bitPosition = bitIndex % Long.SIZE;

            long mask = 1L << bitPosition;
            if ((longs[longIndex] & mask) == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Вычисляет хеш-код для массива байт с заданным сдвигом (seed).
     *
     * @param data массив байт для хеширования
     * @param seed сдвиг для получения разных хешей
     * @return хеш-код в формате {@code int}
     */
    public static int hashCode(MemorySegment data, int length, int seed) {
        return (int) MurmurHash2.hash64(data, length, seed);
    }
}
