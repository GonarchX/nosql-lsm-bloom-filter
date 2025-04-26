package ru.vk.itmo.reference.bloomfilter;

// source https://commons.apache.org/proper/commons-codec/apidocs/src-html/org/apache/commons/codec/digest/MurmurHash2.html#line-228
public class MurmurHash2 {
    private static final long M64 = 0xc6a4a7935bd1e995L;
    private static final int R64 = 47;

    /**
     * Gets the little-endian long from 8 bytes starting at the specified index.
     *
     * @param data  The data
     * @param index The index
     * @return The little-endian long
     */
    private static long getLittleEndianLong(final byte[] data, final int index) {
        return ((long) data[index] & 0xff) |
                ((long) data[index + 1] & 0xff) << 8 |
                ((long) data[index + 2] & 0xff) << 16 |
                ((long) data[index + 3] & 0xff) << 24 |
                ((long) data[index + 4] & 0xff) << 32 |
                ((long) data[index + 5] & 0xff) << 40 |
                ((long) data[index + 6] & 0xff) << 48 |
                ((long) data[index + 7] & 0xff) << 56;
    }

    /**
     * Generates a 64-bit hash from byte array of the given length and seed.
     *
     * @param data   The input byte array
     * @param length The length of the array
     * @param seed   The initial seed value
     * @return The 64-bit hash of the given array
     */
    public static long hash64(final byte[] data, final int length, final int seed) {
        long h = (seed & 0xffffffffL) ^ length * M64;
        final int nblocks = length >> 3;
        // body
        for (int i = 0; i < nblocks; i++) {
            final int index = i << 3;
            long k = getLittleEndianLong(data, index);

            k *= M64;
            k ^= k >>> R64;
            k *= M64;

            h ^= k;
            h *= M64;
        }
        final int index = nblocks << 3;
        switch (length - index) {
            case 7:
                h ^= ((long) data[index + 6] & 0xff) << 48;
                // falls-through
            case 6:
                h ^= ((long) data[index + 5] & 0xff) << 40;
                // falls-through
            case 5:
                h ^= ((long) data[index + 4] & 0xff) << 32;
                // falls-through
            case 4:
                h ^= ((long) data[index + 3] & 0xff) << 24;
                // falls-through
            case 3:
                h ^= ((long) data[index + 2] & 0xff) << 16;
                // falls-through
            case 2:
                h ^= ((long) data[index + 1] & 0xff) << 8;
                // falls-through
            case 1:
                h ^= (long) data[index] & 0xff;
                h *= M64;
        }
        h ^= h >>> R64;
        h *= M64;
        h ^= h >>> R64;
        return h;
    }
}
