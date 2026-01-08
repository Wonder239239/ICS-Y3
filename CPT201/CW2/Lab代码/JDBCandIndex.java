/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
//package xjtlu.cpt201.JDBC;

/**
 *
 * @author weiwang-mac2
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCandIndex {

    //Database connection parameters
    //Replace following parameter values with one's own
    private static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/yeast_prosite";
    private static final String USER = "root";
    private static final String PASS = "02390239Caoqi";

    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;
    private static String sql = null;
    //Search key to create an index
    private static String searchKey = "num";
    //Search key value used in this example
    private static String searchKeyValue = "7";
    //Variables to computer query time
    private static long begin;
    private static long end;

    private void testJDBCQueryWithoutIndex() throws SQLException {
        //drop existing index
        try{
            sql = "Drop INDEX numIndex ON Orf_Motif";
            stmt.execute(sql);
            System.out.println("Existing index on num dropped.");
        }catch (SQLException e) {
            //Index doesn't exist
            System.out.println("No existing index to drop.");
        }

        //prepare query without an index
        System.out.println("Start Query without an index.");
        sql = "SELECT * FROM Orf_Motif WHERE " + searchKey + "=" + searchKeyValue;
        begin = System.currentTimeMillis();
        rs = stmt.executeQuery(sql);
        System.out.println("-----------------");
        System.out.println("Result is:");
        System.out.println("-----------------");
        System.out.println(" first col" + "\t" + " second col");
        System.out.println("-----------------");

        //print actual data from the query
        while (rs.next()) {
            System.out.println(rs.getString("acc_num") + "\t" + rs.getString("orf"));
        }
        end = System.currentTimeMillis();
        double time = end - begin;
        System.out.println("End query without an index.");
        System.out.printf("End query without an index. Time=%.3f ms%n", time);
    }

    private void testJDBCQueryWithIndex() throws SQLException {
        try {
            sql = "CREATE INDEX numIndex ON Orf_Motif (num)";
            stmt.execute(sql);
            System.out.println("Index numIndex created on Orf_Motif(num).");
        } catch (SQLException e) {
            System.out.println("Index numIndex already exists or cannot be created: " + e.getMessage());
        }

        System.out.println("Start Query with an index.");

        sql = "SELECT * FROM Orf_Motif WHERE " + searchKey + "=" + searchKeyValue;
        begin = System.currentTimeMillis();
        rs = stmt.executeQuery(sql);
        System.out.println("-----------------");
        System.out.println("Result is:");
        System.out.println("-----------------");
        System.out.println(" first col" + "\t" + " second col");
        System.out.println("-----------------");


        while (rs.next()) {
            System.out.println(rs.getString("acc_num") + "\t" + rs.getString("orf"));
        }
        end = System.currentTimeMillis();
        double time = end - begin;
        System.out.printf("End query with an index. Time=%.3f ms%n", time);
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        JDBCandIndex testInstance = new JDBCandIndex();

        Class.forName(JDBC_DRIVER);

        conn = DriverManager.getConnection(DB_URL, USER, PASS);
        if (!conn.isClosed()) {
            System.out.println("Succeeded connecting to Database.");
        }

        stmt = conn.createStatement();

        //TEST QUERY WITHOUT AN INDEX
        testInstance.testJDBCQueryWithoutIndex();
        //END TEST QUERY WITHOUT AN INDEX

        //TEST QUERY WITH AN INDEX
        testInstance.testJDBCQueryWithIndex();
        //END TEST QUERY WITH AN INDEX

        //close database connections.
        rs.close();
        stmt.close();
        conn.close();
    }

}
