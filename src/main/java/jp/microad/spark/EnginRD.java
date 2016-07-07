package jp.microad.spark;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

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
		final String tableNameUrlInfo = "url_info";
		final String tableNameErrors = "errors";

		JavaSparkContext jsc = Common.getJavaSparkContext(master);

		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

		JavaRDD<byte[]> deleteRdd = Common.getDeleteRdd(tableNameErrors,
				hbaseContext);
		hbaseContext.bulkDelete(deleteRdd, tableNameErrors,
				new DeleteFunction(), 4);

		List<byte[]> getList = Common.getGetList(tableName, hbaseContext);
		JavaRDD<byte[]> getRdd = jsc.parallelize(getList, 100);

		hbaseContext.foreachPartition(getRdd,
				new VoidFunction<Tuple2<Iterator<byte[]>, HConnection>>() {

					private static final long serialVersionUID = 2432024962766464465L;

					public void call(Tuple2<Iterator<byte[]>, HConnection> t)
							throws Exception {
						HTableInterface table1 = t._2.getTable(Bytes
								.toBytes(tableNameUrlInfo));
						HTableInterface table2 = t._2.getTable(Bytes
								.toBytes(tableNameErrors));
						ContentExtractor ce = new ContentExtractor(true, true,
								0, t._2);

						while (t._1().hasNext()) {
							byte[] b = t._1().next();
							Result r = table1.get(new Get(b));
							byte[] rawHtmlByte = r.getColumnLatestCell(
									Bytes.toBytes("raw_html"), null)
									.getValueArray();

							try {
								HashMap<String, String> re = ce.analyse(Bytes
										.toString(rawHtmlByte));

								String title = re.get("meta_title");
								String desc = re.get("meta_description");
								String metaKeywords = re.get("meta_keywords");
								String text = re.get("main_text");
								String keywords = re.get("keywords");

								Put put1 = new Put(b);
								put1.add(Bytes.toBytes("meta"),
										Bytes.toBytes("title"),
										Bytes.toBytes(title));
								put1.add(Bytes.toBytes("meta"),
										Bytes.toBytes("description"),
										Bytes.toBytes(desc));
								put1.add(Bytes.toBytes("meta"),
										Bytes.toBytes("keywords"),
										Bytes.toBytes(metaKeywords));
								put1.add(Bytes.toBytes("main_text"), null,
										Bytes.toBytes(text));
								put1.add(Bytes.toBytes("keywords"), null,
										Bytes.toBytes(keywords));
								table1.put(put1);
							} catch (java.lang.OutOfMemoryError e1){
								Put put1 = new Put(b);
								put1.add(Bytes.toBytes("message"), null,
										Bytes.toBytes("OutOfMemoryError"));
								table2.put(put1);
							}
							catch (Exception e) {
								Put put1 = new Put(b);
								put1.add(Bytes.toBytes("message"), null,
										Bytes.toBytes(e.getMessage()));
								table2.put(put1);
							}
						}

					}
				});

	}

	public static class DeleteFunction implements Function<byte[], Delete> {

		private static final long serialVersionUID = 1L;

		public Delete call(byte[] v) throws Exception {

			return new Delete(v);
		}

	}

}