package jp.microad.spark;

import scala.Serializable;

//public class RowInfo {
public class RowInfo implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String row;
	private String family;
	private String qualifier;
	public String getRow() {
		return row;
	}
	public void setRow(String row) {
		this.row = row;
	}
	public String getFamily() {
		return family;
	}
	public void setFamily(String family) {
		this.family = family;
	}
	public String getQualifier() {
		return qualifier;
	}
	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	private String value;
}
