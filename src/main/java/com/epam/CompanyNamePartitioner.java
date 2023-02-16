package com.epam;

import com.google.common.base.Preconditions;
import org.apache.spark.Partitioner;

import java.io.Serializable;

/**
 * Puts data for particular company into same partition.
 * Multiple companies into same partition is also possible.
 */
public class CompanyNamePartitioner extends Partitioner implements Serializable {

    private final int partitionCount;

    public CompanyNamePartitioner(int partitionCount) {
        Preconditions.checkArgument(partitionCount >= 1, "Invalid partition count");
        this.partitionCount = partitionCount;
    }

    @Override
    public int numPartitions() {
        return partitionCount;
    }

    @Override
    public int getPartition(Object key) {
        // company = companyName_companyValue
        String company = key.toString();
        // company = companyName
        if (key.toString().contains("_")) {
            company = key.toString().substring(0, key.toString().lastIndexOf("_"));
        }
        // data for particular company must congregates into same partition
        return company.hashCode() % partitionCount;
    }
}
