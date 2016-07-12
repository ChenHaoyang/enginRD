package jp.co.microad.spark;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import jp.co.microad.spark.common.Config;
import jp.co.microad.spark.common.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
import scala.Tuple3;

import com.cloudera.spark.hbase.JavaHBaseContext;
import com.mad.ContentExtractor.ContentExtractor;

public class EnginRD {

    /**
     * メーン メソッド
     * 
     * @param args
     */
    public static void main(String[] args) {

        JavaSparkContext jsc = getJavaSparkContext();
        Configuration conf = getHbaseConffiguration();
        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

        // 異常テーブルデータを削除
        JavaRDD<byte[]> deleteRdd = getRdd(Constants.HBASE_TABLE_NAME_ERRORS, hbaseContext);
        hbaseContext.bulkDelete(deleteRdd, Constants.HBASE_TABLE_NAME_ERRORS, new DeleteFunction(),
                Config.getInt("bulk.delete.batch.size", Constants.DEFAULT_BULK_DELETE_BATCH_SIZE));

        // URL_INFOテーブルからデータを取得
        List<byte[]> parallelizeList = getRdd(Constants.HBASE_TABLE_NAME_URL_INFO, hbaseContext).collect();
        JavaRDD<byte[]> getRdd = jsc.parallelize(parallelizeList,
                Config.getInt("partition.count", Constants.DEFAULT_PARTITION_COUNT));

        hbaseContext.foreachPartition(getRdd, new VoidFunction<Tuple2<Iterator<byte[]>, HConnection>>() {

            private static final long serialVersionUID = 1L;

            public void call(Tuple2<Iterator<byte[]>, HConnection> t) throws Exception {

                HTableInterface urlInfoTable = t._2.getTable(Bytes.toBytes(Constants.HBASE_TABLE_NAME_URL_INFO));
                HTableInterface errorTable = t._2.getTable(Bytes.toBytes(Constants.HBASE_TABLE_NAME_ERRORS));

                ContentExtractor contentExtractor = new ContentExtractor(true, true, 0, t._2);

                while (t._1().hasNext()) {
                    byte[] rowId = t._1().next();
                    try {
                        Result result = urlInfoTable.get(new Get(rowId));
                        byte[] rawHtmlByte = result.getColumnLatestCell(
                                Bytes.toBytes("raw_html"), null).getValueArray();

                        HashMap<String, String> analysedResult = contentExtractor.analyse(Bytes.toString(rawHtmlByte));
                        String title = analysedResult.get("meta_title");
                        String desc = analysedResult.get("meta_description");
                        String metaKeywords = analysedResult.get("meta_keywords");
                        String text = analysedResult.get("main_text");
                        String keywords = analysedResult.get("keywords");

                        Put put = new Put(rowId);
                        put.add(Bytes.toBytes("meta"), Bytes.toBytes("title"), Bytes.toBytes(title));
                        put.add(Bytes.toBytes("meta"), Bytes.toBytes("description"), Bytes.toBytes(desc));
                        put.add(Bytes.toBytes("meta"), Bytes.toBytes("keywords"), Bytes.toBytes(metaKeywords));
                        put.add(Bytes.toBytes("main_text"), null, Bytes.toBytes(text));
                        put.add(Bytes.toBytes("keywords"), null, Bytes.toBytes(keywords));
                        urlInfoTable.put(put);
                    } catch (Throwable throwable) {
                        String message = "エラーメッセージはNULLです。";
                        if (throwable != null && throwable.getMessage() != null) {
                            message = throwable.getMessage();
                        }
                        Put put = new Put(rowId);
                        put.add(Bytes.toBytes("message"), null, Bytes.toBytes(message));
                        errorTable.put(put);
                    }
                }

            }
        });

    }

    /**
     * JavaSparkContextを取得
     * 
     * @return
     */
    public static JavaSparkContext getJavaSparkContext() {
        SparkConf sc = new SparkConf();
        sc.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        return new JavaSparkContext(sc);
    }

    /**
     * hbase confを取得
     * 
     * @return
     */
    private static Configuration getHbaseConffiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path(Config.getProperty("hbase.conf.core-site.xml")));
        conf.addResource(new Path(Config.getProperty("hbase.conf.hbase-site.xml")));
        return conf;
    }

    /**
     * テーブル名より、rddを取得
     * 
     * @param tableName
     * @param hbaseContext
     * @return
     */
    public static JavaRDD<byte[]> getRdd(String tableName,
            JavaHBaseContext hbaseContext) {
        Scan scan = new Scan();
        scan.setCaching(Config.getInt("scan.count", Constants.DEFAULT_SCAN_COUNT));

        JavaRDD<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> javaRdd = hbaseContext.hbaseRDD(tableName, scan);

        return javaRdd.map(new Function<Tuple2<byte[],
                List<Tuple3<byte[], byte[], byte[]>>>, byte[]>() {

                    public byte[] call(
                            Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>> v1)
                            throws Exception {
                        return (byte[])v1._1;
                    }

                });
    }

    public static class DeleteFunction implements Function<byte[], Delete> {

        private static final long serialVersionUID = 1L;

        /**
         * 削除メソッド
         */
        public Delete call(byte[] v) throws Exception {

            return new Delete(v);
        }
    }

}
