package jp.microad.spark;

import scala.Serializable;

public class UrlInfo implements Serializable   {
	
	private static final long serialVersionUID = 1L;
	private String row;
	private String rawHtml;

	public String getRow() {
		return row;
	}
	public void setRow(String row) {
		this.row = row;
	}
	public String getRawHtml() {
		return rawHtml;
	}

	public void setRawHtml(String rawHtml) {
		this.rawHtml = rawHtml;
	}
}
