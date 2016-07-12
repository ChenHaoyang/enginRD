package jp.co.microad.spark.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 設定取得用ユーティルクラス
 */
public class Config {
    private static Properties prop = new Properties();

    static {
        InputStream in = null;
        try {
            ClassLoader cloader = Thread.currentThread().getContextClassLoader();
            if (cloader == null) {
                cloader = Config.class.getClassLoader();
            }
            in = cloader.getResourceAsStream("config.xml");
            prop.loadFromXML(in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * 属性値を戻す
     * 
     * @param propertyName
     * @return
     */
    public static String getProperty(String propertyName) {
        return prop.getProperty(propertyName);
    }

    /**
     * Int属性値を戻す
     * 
     * @param propertyName
     * @param defaultValue
     * @return
     */
    public static int getInt(String propertyName, int defaultValue) {
        try {
            return Integer.parseInt(prop.getProperty(propertyName));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

}
