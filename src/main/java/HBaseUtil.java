/**
 * @author Siqi Wang siqiw1 on 3/28/16.
 */
public class HBaseUtil {
    private static final String TEAM_ID = "SilverLining";
    private static final String TEAM_AWS_ACCOUNT = "6408-5853-5216";

    public static String formatOutput() {
        String header = TEAM_ID + "," + TEAM_AWS_ACCOUNT + "\n";
        return header + "\n";
    }
}
