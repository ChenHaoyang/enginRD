package jp.microad.spark;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple3;

import com.cloudera.spark.hbase.JavaHBaseContext;

public class Common {

	public static JavaRDD<byte[]> getGetRdd(String tableName,
			JavaHBaseContext hbaseContext) {
		Scan scan = new Scan();
	    scan.setCaching(50);
	    
	    JavaRDD<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> javaRdd = hbaseContext.hbaseRDD(tableName, scan);
	    
	    return javaRdd.map(new Function<Tuple2<byte[], 
	    		 List<Tuple3<byte[], byte[], byte[]>>>, byte[]>(){

					public byte[] call(
							Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>> v1)
							throws Exception {
						return (byte[])v1._1;
					}
	    	
	    });
	    
	}
	
	public static JavaRDD<byte[]> getDeleteRdd(String tableName,
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
	    	
	    });
	    
	}
	
	public static List<byte[]> getGetList(String tableName,
			JavaHBaseContext hbaseContext) {
		Scan scan = new Scan();
	    scan.setCaching(50);
	    
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
	
	public static JavaSparkContext getJavaSparkContext(String master) {
		SparkConf sc = new SparkConf();
		sc.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//		sc.set("spark.kryo.registrationRequired", "true");
//		sc.set("spark.kryo.registrator", "jp.microad.spark.MyRegistrator");
		
//		JavaSparkContext jsc = new JavaSparkContext(master,
//				"JavaHBaseDistributedScan", sc);
		
		JavaSparkContext jsc = new JavaSparkContext(sc);
		return jsc;
	}
	
	public static JavaHBaseContext getJavaHbaseContext(JavaSparkContext jsc) {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
		return hbaseContext;
	}
}
