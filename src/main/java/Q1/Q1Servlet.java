package Q1;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Q1Servlet extends HttpServlet {
    private static final String TEAM_ID = "SilverLining";
    private static final String TEAM_AWS_ACCOUNT = "6408-5853-5216";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String key = req.getParameter("key");
        String message = req.getParameter("message");

        PrintWriter writer = resp.getWriter();
        writer.write(formatResponse(decryptMsg(key, message)));
        writer.close();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }

    static String formatResponse(String msg) {
        return String.format("%s,%s\n%s\n%s\n", TEAM_ID, TEAM_AWS_ACCOUNT, DATE_FORMAT.format(new Date()), msg);
    }

    static String decryptMsg(String key, String message) {
        char[][] caesarReversed = decipherCaesar(key, message);
        String spiralReversed = decipherSpiral(caesarReversed[0].length, caesarReversed);
        return spiralReversed;
    }

    /**
     * Reverse cipher step 1 for Caesarify.
     *
     * @param key
     * @param message
     * @return 2-D array of spiral structure.
     */
    static char[][] decipherCaesar(String key, String message) {
        BigInteger keyX = new BigInteger("64266330917908644872330635228106713310880186591609208114244758680898150367880703152525200743234420230");
        BigInteger keyY = new BigInteger(key);
        BigInteger keyZ = keyY.gcd(keyX);

        int keyK = keyZ.mod(new BigInteger("25")).intValue() + 1;
        char[] msgChar = message.toCharArray();
        int sideLength = (int) Math.sqrt(message.length());
        char[][] msgArray = new char[sideLength][sideLength];

        for (int i = 0; i < msgChar.length; i++) {
            if (msgChar[i] - keyK < 'A') {
                msgArray[i / sideLength][i % sideLength] = (char) (msgChar[i] - keyK + 26);
            } else {
                msgArray[i / sideLength][i % sideLength] = (char) (msgChar[i] - keyK);
            }
        }
        return msgArray;
    }

    /**
     * Reverse cipher step 2 of spiralize
     *
     * @param side
     *            side length of spiral 2-D array
     * @param arr
     * @return the decrypted message
     */
    static String decipherSpiral(int side, char[][] arr) {
        int up = 0;
        int right = side;
        int down = side;
        int left = 0;
        int round = 0;
        StringBuilder stringBuilder = new StringBuilder();

        while (side / 2 > round) {
            for (int i = left; i < right - 1; i++) {
                stringBuilder.append(arr[up][i]);
            }

            for (int i = up; i < down - 1; i++) {
                stringBuilder.append(arr[i][right - 1]);
            }

            for (int i = right - 1; i > left; i--) {
                stringBuilder.append(arr[down - 1][i]);
            }

            for (int i = down - 1; i > up; i--) {
                stringBuilder.append(arr[i][left]);
            }
            up++;
            left++;
            right--;
            down--;
            round++;
        }

        if (right - left == 1 && down - up == 1) {
            stringBuilder.append(arr[up][left]);
        }
        return stringBuilder.toString();
    }
}
