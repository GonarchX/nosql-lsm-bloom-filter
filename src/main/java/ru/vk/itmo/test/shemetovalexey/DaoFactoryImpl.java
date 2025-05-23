package ru.vk.itmo.test.shemetovalexey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.shemetovalexey.InMemoryDao;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 5)
public class DaoFactoryImpl implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) throws IOException {
        return new InMemoryDao(config);
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        byte[] bytes = memorySegment.toArray(ValueLayout.OfByte.JAVA_BYTE);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
