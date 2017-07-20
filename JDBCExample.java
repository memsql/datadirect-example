import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class JDBCExample {
  public static void main(String[] args) throws SQLException, IOException {
    // Establish the Connection
    String url = "jdbc:datadirect:mysql://localhost:3306;DatabaseName=test";
    Connection conn = DriverManager.getConnection(url, "root", "");

    // Verify the Connection
    DatabaseMetaData metaData = conn.getMetaData();
    System.out.println("Driver Name: " + metaData.getDriverName());
    System.out.println("Driver Version: " + metaData.getDriverVersion());
    System.out.println("Database Name: " + metaData.getDatabaseProductName());
    System.out.println("Database Version: " + metaData.getDatabaseProductVersion());

    // Set up locals
    Statement stmt = conn.createStatement();
    ResultSet rs;

    // Simple table test
    stmt.execute("CREATE TABLE people (firstname VARCHAR(20), lastname VARCHAR(20), age INT)");
    stmt.executeUpdate("INSERT INTO people VALUES ('John', 'Doe', 25)");

    // Prepared statement test
    PreparedStatement select = conn.prepareStatement("SELECT * FROM people WHERE firstname LIKE ?");
    select.setString(1, "john");
    rs = select.executeQuery();
    while (rs.next()) {
      String first = rs.getString("firstname");
      String last = rs.getString("lastname");
      int age = rs.getInt("age");
      System.out.printf("%s %s %s \n", first, last, age);
    }

    stmt.executeUpdate("DROP TABLE people");

    // Switch database
    stmt.execute("USE test2");

    // Types test
    stmt.executeUpdate("DROP TABLE IF EXISTS bigtable");
    String create = "CREATE TABLE bigtable ("
      + "t_bit BIT, "
      + "t_tinyint TINYINT, "
      + "t_smallint SMALLINT, "
      + "t_int INT, "
      + "t_integer INTEGER, "
      + "t_bigint BIGINT, "
      + "t_real REAL, "
      + "t_double DOUBLE, "
      + "t_decimal DECIMAL(20,10), "
      + "t_numeric NUMERIC, "
      + "t_timestamp TIMESTAMP, "
      + "t_timestamp_six TIMESTAMP(6), "
      + "t_datetime DATETIME, "
      + "t_datetime_six DATETIME(6), "
      + "t_date DATE, "
      + "t_time TIME, "
      + "t_char CHAR, "
      + "t_varchar VARCHAR(20), "
      + "t_tinyblob TINYBLOB, "
      + "t_blob BLOB, "
      + "t_mediumblob MEDIUMBLOB, "
      + "t_longblob LONGBLOB, "
      + "t_tinytext TINYTEXT, "
      + "t_text  TEXT, "
      + "t_mediumtext MEDIUMTEXT, "
      + "t_longtext LONGTEXT, "
      + "t_enum ENUM('e1', 'e2'), "
      + "t_set SET('s1', 's2'), "
      + "t_json JSON "
      + ")";

    String insert = "INSERT INTO bigtable VALUES ("
      + "1, "
      + "127, "
      + "32767, "
      + "2147483647, "
      + "2147483647, "
      + "9223372036854775807, "
      + "12345.6789, "
      + "12345.6789, "
      + "100.3, "
      + "100.3, "
      + "'2017-07-19 10:20:03', "
      + "'2017-07-19 10:20:03.123456', "
      + "'2040-01-19 01:02:03', "
      + "'2040-01-19 01:02:03.654321', "
      + "'2040-01-19', "
      + "'100:59:59', "
      + "'a', "
      + "'something', "
      + "'blob_tiny', "
      + "'blob_blob', "
      + "'blob_medium', "
      + "'blob_long', "
      + "'text_tiny', "
      + "'text_text', "
      + "'text_medium', "
      + "'text_long', "
      + "'e1', "
      + "'s1', "
      + "'{\"key\": \"val\"}' "
      + ")";

    stmt.executeUpdate(create);
    stmt.executeUpdate(insert);

    rs = stmt.executeQuery("SELECT * FROM bigtable");
    while (rs.next()) {
      byte[] bit = new byte[8];
      InputStream s_bit = rs.getBinaryStream("t_bit");
      s_bit.read(bit);
      s_bit.close();
      System.out.println(Arrays.toString(bit));

      System.out.println(rs.getInt("t_tinyint"));
      System.out.println(rs.getInt("t_smallint"));
      System.out.println(rs.getInt("t_int"));
      System.out.println(rs.getInt("t_integer"));
      System.out.println(rs.getLong("t_bigint"));

      System.out.println(rs.getBigDecimal("t_real"));
      System.out.println(rs.getDouble("t_double"));
      System.out.println(rs.getBigDecimal("t_decimal"));
      System.out.println(rs.getBigDecimal("t_numeric"));

      System.out.println(rs.getTimestamp("t_timestamp"));
      System.out.println(rs.getTimestamp("t_timestamp_six"));
      System.out.println(rs.getTimestamp("t_datetime"));
      System.out.println(rs.getTimestamp("t_datetime_six"));
      System.out.println(rs.getDate("t_date"));
      System.out.println(rs.getTime("t_time"));

      char[] character = new char[1];
      Reader s_char = rs.getCharacterStream("t_char");
      s_char.read(character);
      s_char.close();
      System.out.println(character[0]);

      System.out.println(rs.getString("t_varchar"));

      byte[] tinyblob = new byte[9];
      InputStream s_tinyblob = rs.getBlob("t_tinyblob").getBinaryStream();
      s_tinyblob.read(tinyblob);
      s_tinyblob.close();
      System.out.println(Arrays.toString(tinyblob));

      byte[] blob = new byte[9];
      InputStream s_blob = rs.getBlob("t_blob").getBinaryStream();
      s_blob.read(blob);
      s_blob.close();
      System.out.println(Arrays.toString(blob));

      byte[] mediumblob = new byte[11];
      InputStream s_mediumblob = rs.getBlob("t_mediumblob").getBinaryStream();
      s_mediumblob.read(mediumblob);
      s_mediumblob.close();
      System.out.println(Arrays.toString(mediumblob));

      byte[] longblob = new byte[9];
      InputStream s_longblob = rs.getBlob("t_longblob").getBinaryStream();
      s_longblob.read(longblob);
      s_longblob.close();
      System.out.println(Arrays.toString(longblob));

      System.out.println(rs.getString("t_tinytext"));
      System.out.println(rs.getString("t_text"));
      System.out.println(rs.getString("t_mediumtext"));
      System.out.println(rs.getString("t_longtext"));

      System.out.println(rs.getString("t_enum"));
      System.out.println(rs.getString("t_set"));

      System.out.println(rs.getString("t_json"));
    }

    stmt.executeUpdate("DROP TABLE bigtable");

    // System variables test
    stmt.executeUpdate("SET GLOBAL autocommit = on");
    rs = stmt.executeQuery("SELECT @@GLOBAL.autocommit");
    while (rs.next()) {
      System.out.println(rs.getString("@@GLOBAL.autocommit"));
    }

    conn.close();
  }
}
