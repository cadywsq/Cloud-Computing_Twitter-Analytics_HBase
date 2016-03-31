package Q3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class HbaseQuery3DAO {

    private static String zkAddr = "127.0.0.1";
    private static Level logLevel = Level.WARN;

    private static Connection conn;
    private static TableName hbaseTable = TableName.valueOf("tweets3");

    private final static byte[] bColFamily = Bytes.toBytes("data");
    private final static byte[] rowKeyCol = Bytes.toBytes("id");
    private final static byte[] bCol = Bytes.toBytes("output");

    private final static Logger logger = Logger.getRootLogger();
    private static Configuration conf;

    public HbaseQuery3DAO() throws IOException {
        initializeConnection();
    }

    /**
     * Initialize HBase connection.
     *
     * @throws IOException
     */
    private static void initializeConnection() throws IOException {
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

    static String getWordCounts(String userId1, String userId2, String date1, String date2, String word1, String
            word2, String word3) throws Exception {
        try (TweetsTable tweetTable = TweetsTable.getTweetTable()) {
            String wordCountOutput = tweetTable.getWordCountOutput(userId1, userId2, date1, date2, word1, word2, word3);
            return wordCountOutput;
        }
    }


    private static class TweetsTable implements AutoCloseable {
        private final Table tweetTable;

        TweetsTable(Table tweetTable) {
            this.tweetTable = tweetTable;
        }

        private static TweetsTable getTweetTable() throws IOException {
            Table table = conn.getTable(hbaseTable);
            return new TweetsTable(table);
        }

        // Return the 3 words and their counts in required output format.
        private String getWordCountOutput(String userId1, String userId2, String date1, String date2, String word1, String
                word2, String word3) throws IOException {
            String wordCounts = getDateValidLines(userId1, userId2, date1, date2);

            int count1, count2, count3;
            String[] wordCountList = wordCounts.split(";");
            HashMap<String, Integer> wordCountMap = getWordCountMap(wordCountList, word1, word2, word3);

            count1 = wordCountMap.get(word1);
            count2 = wordCountMap.get(word2);
            count3 = wordCountMap.get(word3);

            return String.format("%s:%s\n%s:%s\n%s:%s\n", word1, count1, word2, count2, word3, count3);
        }

        // Return all cells within the required userID range.
        private ArrayList<String> readData(String userId1, String userId2) throws IOException {
            ArrayList<String> dataList = new ArrayList<>();
            userId1 = String.format("%010d", Long.parseLong(userId1));
            userId2 = String.format("%010d", Long.parseLong(userId2) + 1);

            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(userId1));
            scan.setStopRow(Bytes.toBytes(userId2));
            scan.addColumn(bColFamily, rowKeyCol);
            scan.addColumn(bColFamily, bCol);
            scan.setBatch(10);

            ResultScanner resultScanner = tweetTable.getScanner(scan);
            for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
                dataList.add(new String(result.value()));
            }
            return dataList;
        }

        // Return all lines of words and counts within the required time slot.
        private String getDateValidLines(String userId1, String userId2, String date1, String date2) throws IOException {
            ArrayList<String> dataLines = readData(userId1, userId2);
            StringBuilder builder = new StringBuilder();

            for (String dataLine : dataLines) {
                String[] dateLines = dataLine.split("\\|");

                for (String dateLine : dateLines) {
                    String date = dateLine.split(",")[0];
                    if (date.compareTo(date1) >= 0 && date.compareTo(date2) <= 0) {
                        // append word:count text of valid date to string builder.
                        builder.append(dateLine.split(",")[1] + ";");
                    }
                }
            }
            return builder.toString();
        }

        // Return the map of required words as key, count as value.
        private HashMap<String, Integer> getWordCountMap(String[] wordCountList, String word1, String word2, String word3) {
            HashMap<String, Integer> wordCountMap = new HashMap<>();
            wordCountMap.put(word1, 0);
            wordCountMap.put(word2, 0);
            wordCountMap.put(word3, 0);

            for (String wordCount : wordCountList) {
                if (!wordCount.equals("")) {
                    String[] tokens = wordCount.split(":");
                    String word = tokens[0];
                    int count = Integer.valueOf(tokens[1]);

                    if (word.equals(word1) || word.equals(word2) || word.equals(word3)) {
                        wordCountMap.put(word, wordCountMap.get(word) + count);
                    }
                }
            }
            return wordCountMap;
        }

        @Override
        public void close() throws Exception {
            if (tweetTable != null) {
                tweetTable.close();
            }
        }
    }
}
