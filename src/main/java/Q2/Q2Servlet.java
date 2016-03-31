package Q2;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;

public class Q2Servlet extends HttpServlet {
    private static final String TEAM_ID = "SilverLining";
    private static final String TEAM_AWS_ACCOUNT = "6408-5853-5216";
    private static final Cache cacheMap = new Cache();
    private static final int MAX_ENTRIES = 3000000;

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String key = request.getParameter("userid");
        String message = request.getParameter("hashtag");
        System.out.println("UserId: " + key + "\t" + "hashtag: " + message);

        response.setContentType("text/plain;charset=UTF-8");
        PrintWriter writer = response.getWriter();

        if (key == null || message == null) {
            writer.write(formatOutput());
            writer.close();
            return;
        }
        String mapKey = key + "," + message;
        String result = cacheMap.get(mapKey);
        if (result == null) {
            result = formatOutput(key, message);
            cacheMap.put(mapKey, result);
        }
        writer.write(result);
        writer.close();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }

    static String formatOutput(String uid, String hashtag) {
        String matchedTweets = null;
        try {
            matchedTweets = HbaseQuery2DAO.findMatchedTweets(uid, hashtag);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String header = TEAM_ID + "," + TEAM_AWS_ACCOUNT + "\n";
        // If no match is found in database
        if (matchedTweets == null || matchedTweets.isEmpty()) {
            return header + "\n";
        }
        matchedTweets = matchedTweets.replace("\\n", "\n").replace("\\t", "\t").replace("\\\"", "\"");
        return header + matchedTweets + "\n\n";
    }

    static String formatOutput() {
        String header = TEAM_ID + "," + TEAM_AWS_ACCOUNT + "\n";
        return header + "\n";
    }

    /**
     * Override LinkedHashMap removeEldestEntry method & override get/put method for synchronization.
     */
    static class Cache extends LinkedHashMap<String, String> {
        // Set initial size to avoid LinkedHashMap to resize.
        public Cache() {
            super((int) (MAX_ENTRIES / 0.75 + 1), 0.75f, true);
        }

        @Override
        protected boolean removeEldestEntry(final Map.Entry<String, String> cacheMap) {
            return size() > MAX_ENTRIES;
        }

        @Override
        public synchronized String get(Object key) {
            return super.get(key);
        }

        @Override
        public synchronized String put(String key, String value) {
            return super.put(key, value);
        }
    }

}
