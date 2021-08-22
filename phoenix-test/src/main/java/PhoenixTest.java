import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @program: gmallrealtime-parent
 * @description: Phoenix连接测试
 * @create: 2021-08-22 13:11
 */


public class PhoenixTest {
    public static void main(String[] args) throws Exception {

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        Connection conn = DriverManager.getConnection("jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181");

        PreparedStatement preparedStatement = conn.prepareStatement("select * from 'test'");

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + ":" + resultSet.getString(2) + ":" + resultSet.getString(3));
        }
        resultSet.close();
        preparedStatement.close();
        conn.close();

    }
}
