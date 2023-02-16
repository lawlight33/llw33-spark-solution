package com.epam;

import org.apache.spark.sql.Row;

import java.io.Serializable;

public class RowWrapper implements Serializable {

    private Row row;

    public RowWrapper(Row initialRow) {
        this.row = initialRow;
    }

    public String getAs(int i) {
        return row.getAs(i);
    }

    public void modifyRow(Row newRow) {
        this.row = newRow;
    }

    @Override
    public String toString() {
        String rowStr = row.toString();
        return rowStr.substring(1, rowStr.length() - 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowWrapper that = (RowWrapper) o;
        return row.equals(that.row);
    }

    @Override
    public int hashCode() {
        return row.hashCode();
    }
}
