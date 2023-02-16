package com.epam;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class CompanyNamePartitionerTest {

    @Test
    public void partitionTest() {
        CompanyNamePartitioner partitioner = new CompanyNamePartitioner(3);

        assertEquals(3, partitioner.numPartitions());

        // GOOGLE
        assertEquals(1, partitioner.getPartition("GOOGLE_1"));
        assertEquals(1, partitioner.getPartition("GOOGLE_2"));
        assertEquals(1, partitioner.getPartition("GOOGLE_1000"));
        assertEquals(1, partitioner.getPartition("GOOGLE_5000"));
        assertEquals(1, partitioner.getPartition("GOOGLE_99999"));
        assertNotEquals(2, partitioner.getPartition("GOOGLE_1"));
        assertNotEquals(2, partitioner.getPartition("GOOGLE_99999"));
        assertNotEquals(3, partitioner.getPartition("GOOGLE_1"));
        assertNotEquals(3, partitioner.getPartition("GOOGLE_99999"));

        // MICROSOFT
        assertEquals(1, partitioner.getPartition("MICROSOFT_3"));
        assertEquals(1, partitioner.getPartition("MICROSOFT_5555"));
        assertEquals(1, partitioner.getPartition("MICROSOFT_2000"));
        assertEquals(1, partitioner.getPartition("MICROSOFT_100000"));
        assertNotEquals(2, partitioner.getPartition("MICROSOFT_2"));
        assertNotEquals(2, partitioner.getPartition("GOOGLE_888888"));
        assertNotEquals(3, partitioner.getPartition("MICROSOFT_2"));
        assertNotEquals(3, partitioner.getPartition("GOOGLE_888888"));
    }
}
