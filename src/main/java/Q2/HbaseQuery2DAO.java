package Q2;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Siqi Wang siqiw1 on 3/10/16.
 */
public class HbaseQuery2DAO {
    /**
     * The private IP address of HBase master node.
     */
    //TODO: change to master node private IP.
    static String zkAddr = "172.31.48.217";
    static Level logLevel = Level.WARN;

    /**
     * HBase connection.
     */
    static HConnection conn;
    /**
     * Byte representation of column family.
     */
    private final static byte[] bColFamily = Bytes.toBytes("data");
    /**
     * Logger.
     */
    private final static Logger logger = Logger.getRootLogger();
    private static Configuration conf;

    public HbaseQuery2DAO() throws IOException {
        initializeConnection();
    }

    /**
     * Initialize HBase connection.
     *
     * @throws IOException
     */
    static void initializeConnection() throws IOException {
        // Remember to set correct log level to avoid unnecessary output.
        logger.setLevel(logLevel);
        conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
//        conf.set("hbase.master", "*" + zkAddr + ":9000*");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("HBase not configured!");
            return;
        }
        if (conn == null) {
            conn = HConnectionManager.createConnection(conf);
        }
    }

    static String findMatchedTweets(String userId, String hashtag) throws Exception {
        try (TweetsTable table = TweetsTable.getTweetTable()) {
            String matchedTweets = table.get(userId, hashtag);
            return matchedTweets;
        }
    }

    static class TweetsTable implements AutoCloseable {
        private final HTableInterface tweetTable;

        TweetsTable(HTableInterface tweetTable) {
            this.tweetTable = tweetTable;
        }

        static TweetsTable getTweetTable() throws IOException {
            HTableInterface table = conn.getTable("tweets");
            return new TweetsTable(table);
        }

        String get(String userId, String hashtag) throws IOException {
            String queryParam = userId + "," + hashtag;
            Get get = new Get(Bytes.toBytes(queryParam));
            get.addColumn(bColFamily, Bytes.toBytes("idht"));
            get.addColumn(bColFamily, Bytes.toBytes("output"));

            Result result = tweetTable.get(get);
            if (result == null || result.isEmpty()) {
                return "";
            }
//            byte[] value = result.getValue(Bytes.toBytes("useridhashtag"), Bytes.toBytes("output"));
            byte[] value = result.value();
            String matchedTweets = new String(value, Charsets.UTF_8);
            return matchedTweets;
        }

        @Override
        public void close() throws Exception {
            if (tweetTable != null) {
                tweetTable.close();
            }
        }
    }
}
