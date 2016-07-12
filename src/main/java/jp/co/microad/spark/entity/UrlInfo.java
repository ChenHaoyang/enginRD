package jp.co.microad.spark.entity;

import scala.Serializable;

public class UrlInfo implements Serializable {

    private static final long serialVersionUID = 1L;
    private byte[] row;
    private byte[] rawHtml;

    public byte[] getRow() {
        return row;
    }

    public void setRow(byte[] row) {
        this.row = row;
    }

    public byte[] getRawHtml() {
        return rawHtml;
    }

    public void setRawHtml(byte[] rawHtml) {
        this.rawHtml = rawHtml;
    }

}
