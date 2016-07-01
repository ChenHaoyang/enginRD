package jp.microad.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;
import scala.Tuple3;

import com.cloudera.spark.hbase.JavaHBaseContext;
import com.mad.ContentExtractor.ContentExtractor;

public class EnginRD {
	public static void main(String args[]) {
		if (args.length == 0) {
			System.out
					.println("JavaHBaseDistributedScan  {master} {tableName}");
		}

		String master = args[0];
		String tableName = args[1];

		JavaSparkContext jsc = new JavaSparkContext(master,
				"JavaHBaseDistributedScan");
		
//		JavaSparkContext jsc = new JavaSparkContext();

		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

	    List<byte[]> allRowList = getBulkGetRowList(tableName, hbaseContext);

	    JavaRDD<byte[]> getRdd = jsc.parallelize(allRowList);
	    
	    JavaRDD<UrlInfo> urlInfoRdd = hbaseContext.bulkGet(tableName, 2, getRdd, new GetFunction(),
		        new ResultFunction());
	    
	    JavaRDD<List<RowInfo>> listUrlInfoForPutRdd = urlInfoRdd.map(new MapFunction());
	    
	    List<RowInfo> list = listUrlInfoForPutRdd.reduce(new ReduceFunction());

		JavaRDD<RowInfo> putRdd = jsc.parallelize(list);

		hbaseContext.bulkPut(putRdd, tableName, new PutFunction(), true);

	}
	
	public static List<byte[]> getBulkGetRowList(String tableName,
			JavaHBaseContext hbaseContext) {
		Scan scan = new Scan();
	    scan.setCaching(100);
	    
	    JavaRDD<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> javaRdd = hbaseContext.hbaseRDD(tableName, scan);
	    
	    return javaRdd.map(new Function<Tuple2<byte[], 
	    		 List<Tuple3<byte[], byte[], byte[]>>>, byte[]>(){

					public byte[] call(
							Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>> v1)
							throws Exception {
						return (byte[])v1._1;
					}
	    	
	    }).collect();
	    
	}
	
	public static class ReduceFunction implements Function2<List<RowInfo>,List<RowInfo>,List<RowInfo>> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public List<RowInfo> call(List<RowInfo> v1,
				List<RowInfo> v2) throws Exception {
			v1.addAll(v2);
			return v1;
		}
	}
	
	public static class MapFunction implements Function<UrlInfo, List<RowInfo>> {

	    private static final long serialVersionUID = 1L;
	    

	    @SuppressWarnings("unchecked")
		public List<RowInfo> call(UrlInfo v) throws Exception {
	    	
	    	ContentExtractor ce = new ContentExtractor(true, true, true, 0);
		    RowInfo uifp = null;
	    	List<RowInfo> list = new ArrayList<RowInfo>();
	    	//TODO Shared Variables https://spark.apache.org/docs/latest/programming-guide.html#broadcast-variables
	    	HashMap<String, String> re = ce.analyse(v.getRawHtml());

			uifp = new RowInfo();
			uifp.setRow(v.getRow());
			uifp.setFamily("meta");
			uifp.setQualifier("title");
			uifp.setValue(re.get("meta_title"));
			list.add(uifp);

			uifp = new RowInfo();
			uifp.setRow(v.getRow());
			uifp.setFamily("meta");
			uifp.setQualifier("description");
			uifp.setValue(re.get("meta_description"));
			list.add(uifp);

			uifp = new RowInfo();
			uifp.setRow(v.getRow());
			uifp.setFamily("meta");
			uifp.setQualifier("keywords");
			uifp.setValue(re.get("meta_keywords"));
			list.add(uifp);

			uifp = new RowInfo();
			uifp.setRow(v.getRow());
			uifp.setFamily("main_text");
			uifp.setQualifier("");
			uifp.setValue(re.get("main_text"));
			list.add(uifp);

			uifp = new RowInfo();
			uifp.setRow(v.getRow());
			uifp.setFamily("keywords");
			uifp.setQualifier("");
			uifp.setValue(re.get("keywords"));
			list.add(uifp);
	      return list;
	    }
	  }
	
	public static class GetFunction implements Function<byte[], Get> {

	    private static final long serialVersionUID = 1L;

	    public Get call(byte[] v) throws Exception {
	      return new Get(v);
	    }
	  }

	  public static class ResultFunction implements Function<Result, UrlInfo> {

	    private static final long serialVersionUID = 1L;

		public UrlInfo call(Result result) throws Exception {
	      UrlInfo info = new UrlInfo();
	      info.setRow(Bytes.toString(result.getRow()));
	      info.setRawHtml(Bytes.toString(result.getValue(Bytes.toBytes("raw_html"), null)));
	      return info;
	    }
	  }

	public static class PutFunction implements Function<RowInfo, Put> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Put call(RowInfo v1) throws Exception {
			Put put = new Put(Bytes.toBytes(v1.getRow()));

			put.add(Bytes.toBytes(v1.getFamily()),
					Bytes.toBytes(v1.getQualifier()),
					Bytes.toBytes(v1.getValue()));
			return put;
		}

	}
}