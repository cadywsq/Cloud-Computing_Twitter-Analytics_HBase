package Q2;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
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
    private static String zkAddr = "127.0.0.1";
    private static Level logLevel = Level.WARN;

    private static Connection conn;
    private static TableName hbaseTable = TableName.valueOf("tweets");

    private final static byte[] bColFamily = Bytes.toBytes("data");

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
    private static void initializeConnection() throws IOException {
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
        conn = ConnectionFactory.createConnection(conf);
    }

    static String findMatchedTweets(String userId, String hashtag) throws Exception {
        try (TweetsTable table = TweetsTable.getTweetTable()) {
            String matchedTweets = table.get(userId, hashtag);
            return matchedTweets;
        }
    }

    static class TweetsTable implements AutoCloseable {
        private final Table tweetTable;

        TweetsTable(Table tweetTable) {
            this.tweetTable = tweetTable;
        }

        static TweetsTable getTweetTable() throws IOException {
            Table table = conn.getTable(hbaseTable);
            return new TweetsTable(table);
        }

        private String get(String userId, String hashtag) throws IOException {
            String queryParam = userId + "#" + hashtag;
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
