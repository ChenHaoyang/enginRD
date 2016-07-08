package jp.co.microad.spark.common;

/**
 * 定数クラス
 */
public class Constants {

    /** デフォルトスキャン数 **/
    public static final int DEFAULT_SCAN_COUNT = 100;

    /** デフォルトバッチサイズ **/
    public static final int DEFAULT_BULK_DELETE_BATCH_SIZE = 4;

    /** デフォルトパーティション数 **/
    public static final int DEFAULT_PARTITION_COUNT = 100;

    /** URL_INFOテーブル **/
    public static final String HBASE_TABLE_NAME_URL_INFO = "url_info_test_small";

    /** 異常テーブル **/
    public static final String HBASE_TABLE_NAME_ERRORS = "errors";
}
