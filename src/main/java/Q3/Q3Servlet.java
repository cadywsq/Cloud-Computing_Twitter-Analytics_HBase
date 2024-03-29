package Q3;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class Q3Servlet extends HttpServlet {
    private static final String TEAM_ID = "SilverLining";
    private static final String TEAM_AWS_ACCOUNT = "6408-5853-5216";

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String id1 = req.getParameter("start_userid");
        String id2 = req.getParameter("end_userid");
        String date1 = req.getParameter("start_date");
        String date2 = req.getParameter("end_date");
        String[] words = req.getParameter("words").split(",");
        String w1 = words[0];
        String w2 = words[1];
        String w3 = words[2];

        System.out.println(String.format("id1: %s\tid2: %s\tdate1: %s\tdate2: %s", id1, id2, date1, date2));

        String result = null;
        String dateParameter1 = date1.replace("-", "");
        String dateParameter2 = date2.replace("-", "");
        try {
            result = HbaseQuery3DAO.getWordCounts(id1, id2, dateParameter1, dateParameter2, w1, w2, w3);
        } catch (Exception e) {
            e.printStackTrace();
        }
        PrintWriter writer = resp.getWriter();
        writer.write(formatOutput(result));
        writer.close();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }

    static String formatOutput(String wordCount) {
        String header = TEAM_ID + "," + TEAM_AWS_ACCOUNT + "\n";
        return header + wordCount;
    }

}
