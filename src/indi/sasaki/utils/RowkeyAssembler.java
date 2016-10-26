package indi.sasaki.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author sasaki
 * Assemble the tel, lac and cellid to form a new rowkey
 * with a random number salted ahead, decideded by the region num.
 */
public class RowkeyAssembler {
	private String telNum = null;
	private String lac = null;
	private String cellid = null;
	private String timestamp = null;
	private int regions;
	
	public RowkeyAssembler(String tel, String lac, String cellid, String timestamp, int regionNum) {
		this.telNum = tel;
		this.lac = lac;
		this.cellid = cellid;
		this.regions = regionNum;
		this.timestamp = timestamp;
	}
	
	public byte[] saltedKey() throws ParseException {
		String millis = timeMillis(timestamp);
		return Bytes.toBytes(Math.abs(millis.hashCode()) % regions + "|" + telNum + "|"
				+ lac + "|" + cellid + "|" + millis); 
	}
	
/*	public String idExtend(String rawId) {
		String extendedId = rawId;
		int rawSize = rawId.length();
		for (int i = 0; i < (5 - rawSize); i++) {
			extendedId = "0" + extendedId;
		}
		return extendedId;
	}*/
	
	public String timeMillis(String sTime) throws ParseException {
		SimpleDateFormat dateFmtr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	    Long dateMillis = dateFmtr.parse(sTime.substring(0, sTime.length() - 4)).getTime();
	    return dateMillis.toString();
	}
}
