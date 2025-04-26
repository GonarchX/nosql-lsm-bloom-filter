package ru.vk.itmo.reference;

import ru.vk.itmo.Entry;
import ru.vk.itmo.reference.bloomfilter.BloomFilter;
import ru.vk.itmo.reference.bloomfilter.BloomFilterUtils;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;

/**
 * Writes {@link Entry} {@link Iterator} to SSTable on disk.
 *
 * <p>Index file {@code <N>.index} contains {@code long} offsets to entries in data file:
 * {@code [offset0, offset1, ...]}
 *
 * <p>Data file {@code <N>.data} contains serialized entries:
 * {@code <long keyLength><key><long valueLength><value>}
 *
 * <p>Tombstones are encoded as {@code valueLength} {@code -1} and no subsequent value.
 *
 * @author incubos
 */
final class SSTableWriter {
    private static final int BUFFER_SIZE = 64 * 1024;

    // Reusable buffers to eliminate allocations.
    // But excessive memory copying is still there :(
    // Long cell
    private final ByteArraySegment longBuffer = new ByteArraySegment(Long.BYTES);
    // Growable blob cell
    private final ByteArraySegment blobBuffer = new ByteArraySegment(512);

    void write(
            final Path baseDir,
            final int sequence,
            final Iterator<Entry<MemorySegment>> entries) throws IOException {
        // Write to temporary files
        final Path tempFilterName = SSTables.tempFilterName(baseDir, sequence);
        final Path tempIndexName = SSTables.tempIndexName(baseDir, sequence);
        final Path tempDataName = SSTables.tempDataName(baseDir, sequence);

        // Delete temporary files to eliminate tails
        Files.deleteIfExists(tempIndexName);
        Files.deleteIfExists(tempDataName);

        // Здесь представлен типичный трейд-офф: экономить память или цпу. Зависит он от следующих факторов.
        // Один из способов создания Блум фильтра:
        // пройти заранее по всем записям в итераторе, чтобы узнать необходимый размер Блум фильтра.
        // Однако из-за дополнительного прохода процесс записи замедлится,
        // зато размер фильтра будет оптимальным (сэкономим память + проверка наличия записи в фильтре будет проходить с указанной нами вероятностью false-positive).
        //
        // Другой способ:
        // заранее аллоцировать Блум фильтр, не зная сколько на самом деле придет значений.
        // Такой способ быстрее, однако размер фильтра часто будет не соответствовать количеству записей в SSTable:
        // мало записей в SSTable, фильтр большой => избыточный расход памяти,
        // много записей в SSTable, фильтр маленький => чаще false-positive ответы от фильтра.
        //
        // Из-за особенностей референсной реализации, проитерироваться несколько раз не получится,
        // т.к. SSTable итераторы читают данные напрямую из диска. Поэтому создаем Блум фильтр заранее.
        BloomFilter bloomFilter = BloomFilter.newFilter();

        // Iterate in a single pass!
        // Will write through FileChannel despite extra memory copying and
        // no buffering (which may be implemented later).
        // Looking forward to MemorySegment facilities in FileChannel!
        try (OutputStream filter =
                     new BufferedOutputStream(
                             new FileOutputStream(
                                     tempFilterName.toFile()),
                             BUFFER_SIZE);
             OutputStream index =
                     new BufferedOutputStream(
                             new FileOutputStream(
                                     tempIndexName.toFile()),
                             BUFFER_SIZE);
             OutputStream data =
                     new BufferedOutputStream(
                             new FileOutputStream(
                                     tempDataName.toFile()),
                             BUFFER_SIZE)) {
            long entryOffset = 0L;

            // Iterate and serialize
            while (entries.hasNext()) {
                // First write offset to the entry
                writeLong(entryOffset, index);

                // Then write the entry
                final Entry<MemorySegment> entry = entries.next();
                entryOffset += writeEntry(entry, data);

                // Добавляем все записи в BloomFilter, даже удаленные (value == Tombstone).
                // Считаю, что лучше получить информацию о том, что запись удалена, чем посчитать, что ее вообще не было.
                bloomFilter.add(entry.key());
            }

            writeFilter(bloomFilter, filter);
        }

        // Publish files atomically
        final Path filterName =
                SSTables.filterName(
                        baseDir,
                        sequence);
        Files.move(
                tempFilterName,
                filterName,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        final Path indexName =
                SSTables.indexName(
                        baseDir,
                        sequence);
        Files.move(
                tempIndexName,
                indexName,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        final Path dataName =
                SSTables.dataName(
                        baseDir,
                        sequence);
        Files.move(
                tempDataName,
                dataName,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    private void writeLong(
            final long value,
            final OutputStream os) throws IOException {
        longBuffer.segment().set(
                ValueLayout.OfLong.JAVA_LONG_UNALIGNED,
                0,
                value);
        longBuffer.withArray(os::write);
    }

    private void writeSegment(
            final MemorySegment value,
            final OutputStream os) throws IOException {
        final long size = value.byteSize();
        blobBuffer.ensureCapacity(size);
        MemorySegment.copy(
                value,
                0L,
                blobBuffer.segment(),
                0L,
                size);
        blobBuffer.withArray(array ->
                os.write(
                        array,
                        0,
                        (int) size));
    }

    /**
     * Writes {@link Entry} to {@link FileChannel}.
     *
     * @return written bytes
     */
    private long writeEntry(
            final Entry<MemorySegment> entry,
            final OutputStream os) throws IOException {
        final MemorySegment key = entry.key();
        final MemorySegment value = entry.value();
        long result = 0L;

        // Key size
        writeLong(key.byteSize(), os);
        result += Long.BYTES;

        // Key
        writeSegment(key, os);
        result += key.byteSize();

        // Value size and possibly value
        if (value == null) {
            // Tombstone
            writeLong(SSTables.TOMBSTONE_VALUE_LENGTH, os);
            result += Long.BYTES;
        } else {
            // Value length
            writeLong(value.byteSize(), os);
            result += Long.BYTES;

            // Value
            writeSegment(value, os);
            result += value.byteSize();
        }

        return result;
    }

    private void writeFilter(
            BloomFilter bloomFilter,
            OutputStream os) throws IOException {
        // Write filter metadata (filter size, hash functions count)
        long[] longs = BloomFilterUtils.toLongArray(bloomFilter.getBitSet());
        writeLong(longs.length, os);
        writeLong(bloomFilter.getHashCount(), os);

        // Write filter value
        for (Long longPart : longs) {
            writeLong(longPart, os);
        }
    }
}
