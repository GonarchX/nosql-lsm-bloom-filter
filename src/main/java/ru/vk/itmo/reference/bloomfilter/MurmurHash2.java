package ru.vk.itmo.reference.bloomfilter;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Реализация хеш-функции MurmurHash2 для {@link MemorySegment} с использованием {@link ValueLayout#JAVA_LONG_UNALIGNED}.
 * <p>
 * Источник алгоритма: <a href="https://commons.apache.org/proper/commons-codec/apidocs/src-html/org/apache/commons/codec/digest/MurmurHash2.html">Apache Commons Codec</a>.
 */
public class MurmurHash2 {
    private static final long M64 = 0xc6a4a7935bd1e995L;
    private static final int R64 = 47;

    /**
     * Читает значение {@code long} в формате Little-Endian из {@link MemorySegment}.
     *
     * @param segment Сегмент памяти
     * @param index   Смещение в байтах
     * @return Значение long
     */
    private static long getLittleEndianLong(MemorySegment segment, long index) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, index);
    }

    /**
     * Вычисляет 64-битный хеш {@link MemorySegment} с использованием алгоритма MurmurHash2.
     *
     * @param segment Сегмент памяти
     * @param length  Длина данных в байтах
     * @param seed    Начальное значение seed
     * @return 64-битный хеш
     */
    public static long hash64(MemorySegment segment, int length, int seed) {
        long h = (seed & 0xffffffffL) ^ (length * M64);
        final int nblocks = length >> 3;

        // Основная часть обработки блоков по 8 байт
        for (int i = 0; i < nblocks; i++) {
            long k = getLittleEndianLong(segment, (long) i * 8);

            k *= M64;
            k ^= k >>> R64;
            k *= M64;

            h ^= k;
            h *= M64;
        }

        // Обработка оставшихся байт (хвост)
        int index = nblocks << 3;
        long remaining = 0;
        for (int i = 0; i < (length - index); i++) {
            long b = segment.get(ValueLayout.JAVA_BYTE, index + i) & 0xffL;
            remaining ^= b << (i * 8);
        }
        if (length - index > 0) {
            h ^= remaining;
            h *= M64;
        }

        // Финализация
        h ^= h >>> R64;
        h *= M64;
        h ^= h >>> R64;

        return h;
    }
}
