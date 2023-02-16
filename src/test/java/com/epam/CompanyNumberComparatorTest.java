package com.epam;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CompanyNumberComparatorTest {

    @Test
    public void compareTest() {
        CompanyNumberComparator comparator = new CompanyNumberComparator();

        // GOOGLE
        assertEquals(-1, comparator.compare("GOOGLE_1", "GOOGLE_2"));
        assertEquals(-1, comparator.compare("GOOGLE_10", "GOOGLE_555"));
        assertEquals(1, comparator.compare("GOOGLE_8888", "GOOGLE_900"));

        // MICROSOFT
        assertEquals(1, comparator.compare("MICROSOFT_654", "MICROSOFT_270"));
        assertEquals(1, comparator.compare("MICROSOFT_32", "MICROSOFT_0"));
        assertEquals(-1, comparator.compare("MICROSOFT_CORP_58", "MICROSOFT_CORP_3255"));
    }
}
