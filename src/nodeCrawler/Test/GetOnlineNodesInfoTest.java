package nodeCrawler.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import nodeCrawler.NodeCrawlerDb;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetOnlineNodesInfoTest {
    public static void main(String[] args) throws SQLException, IOException {
        NodeCrawlerDb db = new NodeCrawlerDb();
        Connection conn = db.getConnection();
        String sql = "select * from online_nodes";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs =  ps.executeQuery();
        BufferedWriter writer = new BufferedWriter(new FileWriter("online nodes.csv"));
        writer.write("online_time,ipv4,location,server_vendor\n");
        while(rs.next()) {
            String onlineTime = rs.getString("online_time");
            String ipv4 = rs.getString("ipv4");

            // 查询地理位置和服务器厂商
            String location = getGeoLocation(ipv4);
            String serverVendor = getServerVendor(ipv4);

            // 写入 CSV 文件
            writer.write(String.format("%s,%s,%s,%s\n", onlineTime, ipv4, location, serverVendor));

        }
    }
    public static String getGeoLocation(String ipv4) {
        String urlString = "http://ipinfo.io/" + ipv4 + "/json";
        try {
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // 解析 JSON 数据
            JsonObject jsonResponse = JsonParser.parseString(response.toString()).getAsJsonObject();
            return jsonResponse.get("city").getAsString() + ", " + jsonResponse.get("region").getAsString() + ", " + jsonResponse.get("country").getAsString();

        } catch (Exception e) {
            e.printStackTrace();
            return "Unknown Location";
        }
    }
    public static String getServerVendor(String ipv4) {
        String urlString = "http://ipinfo.io/" + ipv4 + "/json";
        try {
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // 解析 JSON 数据
            JsonObject jsonResponse = JsonParser.parseString(response.toString()).getAsJsonObject();
            return jsonResponse.get("org").getAsString(); // 服务器厂商信息（ISP）

        } catch (Exception e) {
            e.printStackTrace();
            return "Unknown Vendor";
        }
    }
}
