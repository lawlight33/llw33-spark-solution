package com.epam;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compares different companies only by its values.
 *   1. Extracts companyNumber both from left and right company key: companyName_companyNumber
 *   2. Compares by companyNumber.
 *
 * Examples:
 *   APPLE_123 > GOOGLE_122
 *   MICROSOFT_155 < MICROSOFT 160
 */
public class CompanyNumberComparator implements Serializable, Comparator<String> {

    @Override
    public int compare(String o1, String o2) {
        long idx1 = Long.parseLong(o1.substring(o1.lastIndexOf("_")+1));
        long idx2 = Long.parseLong(o2.substring(o2.lastIndexOf("_")+1));
        return Long.compare(idx1, idx2);
    }
}