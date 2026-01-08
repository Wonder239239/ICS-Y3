import com.mysql.cj.jdbc.MysqlXADataSource;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.math.BigDecimal;
import java.sql.*;

public class FundTransferJTA {

    private static final String DB_URL =
            "jdbc:mysql://127.0.0.1:3306/transactiondb?useSSL=false&serverTimezone=UTC";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "02390239Caoqi";

    public static void main(String[] args) {

        double amount =100.0;

        MysqlXADataSource xaDs1 = new MysqlXADataSource();
        xaDs1.setUrl(DB_URL);
        xaDs1.setUser(DB_USER);
        xaDs1.setPassword(DB_PASSWORD);

        MysqlXADataSource xaDs2 = new MysqlXADataSource();
        xaDs2.setUrl(DB_URL);
        xaDs2.setUser(DB_USER);
        xaDs2.setPassword(DB_PASSWORD);

        byte[] globalId = new byte[]{0x01};
        byte[] branchIdAlice = new byte[]{0x57};
        byte[] branchIdBob   = new byte[]{0x58};

        Xid xid1 = new MyXid(100, globalId, branchIdAlice);
        Xid xid2 = new MyXid(100, globalId, branchIdBob);

        XAConnection xaCon1 = null;
        XAConnection xaCon2 = null;
        Connection conn1 = null;
        Connection conn2 = null;
        Statement stmt1 = null;
        Statement stmt2 = null;
        XAResource xares1 = null;
        XAResource xares2 = null;

        try {
            xaCon1 = xaDs1.getXAConnection();
            xaCon2 = xaDs2.getXAConnection();

            conn1 = xaCon1.getConnection();
            conn2 = xaCon2.getConnection();

            stmt1 = conn1.createStatement();
            stmt2 = conn2.createStatement();

            xares1 = xaCon1.getXAResource();
            xares2 = xaCon2.getXAResource();

            String table1 = "accounts_Alice";
            String table2 = "accounts_Bob";


            initSchema(conn1, table1);
            initSchema(conn2, table2);

            seedDemoData(conn1, table1, "Alice", 0.0);
            seedDemoData(conn2, table2, "Bob", 50.0);

            System.out.println("\n--- Initial Balance---");
            printBalance(conn1, table1, "Alice");
            printBalance(conn2, table2, "Bob");
            System.out.println("");


            // =================== Phase 1 ===================
            xares1.start(xid1, XAResource.TMNOFLAGS);
            stmt1.execute("UPDATE " + table1 + " SET balance = balance + " + amount + " WHERE id = 1");
            xares1.end(xid1, XAResource.TMSUCCESS);
            xares2.start(xid2, XAResource.TMNOFLAGS);


            BigDecimal currentBalance = selectBalanceForUpdate(conn2, table2, 1);
            BigDecimal transferAmount = BigDecimal.valueOf(amount);

            if (currentBalance == null || currentBalance.compareTo(transferAmount) < 0) {
                System.out.println("Insufficient funds in " + table2 + " → ROLLBACK both branches");
                xares2.end(xid2, XAResource.TMFAIL);

                xares1.rollback(xid1);
                xares2.rollback(xid2);

                System.out.println("Distributed transaction ROLLED BACK.");
                System.out.println("\n--- Final Result ---");
                printBalance(conn1, table1, "Alice");
                printBalance(conn2, table2, "Bob");
                return;
            }

            stmt2.execute("UPDATE " + table2 + " SET balance = balance - " + amount + " WHERE id = 1");
            xares2.end(xid2, XAResource.TMSUCCESS);

            int ret1 = xares1.prepare(xid1);
            int ret2 = xares2.prepare(xid2);

            // =================== Phase 2 ===================
            if (ret1 == XAResource.XA_OK && ret2 == XAResource.XA_OK) {
                xares1.commit(xid1, false);
                xares2.commit(xid2, false);
                System.out.println("Distributed transaction COMMITTED.");
            } else {
                xares1.rollback(xid1);
                xares2.rollback(xid2);
                System.out.println("Prepare failed → ROLLBACK.");
            }

            System.out.println("\n--- Final Result ---");
            printBalance(conn1, table1, "Alice");
            printBalance(conn2, table2, "Bob");

        } catch (SQLException | XAException e) {
            System.out.println("Exception: " + e.getMessage());
        } finally {
            try {
                if (stmt1 != null) stmt1.close();
                if (stmt2 != null) stmt2.close();
                if (conn1 != null) conn1.close();
                if (conn2 != null) conn2.close();
                if (xaCon1 != null) xaCon1.close();
                if (xaCon2 != null) xaCon2.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void initSchema(Connection conn, String tablename) throws SQLException {
        try (Statement st = conn.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS " + tablename + " ("
                            + "id INT PRIMARY KEY,"
                            + "name VARCHAR(100) NOT NULL,"
                            + "balance DECIMAL(19,4) NOT NULL"
                            + ") ENGINE=InnoDB";
            st.execute(sql);
        }
    }

    private static void seedDemoData(Connection conn, String tableName, String ownerName, double initialBalance) throws SQLException {
        String deleteSql = "DELETE FROM " + tableName;
        try (Statement st = conn.createStatement()) {
            st.execute(deleteSql);
        }

        String insertSql = "INSERT INTO " + tableName + " (id, name, balance) VALUES (?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
            ps.setInt(1, 1);
            ps.setString(2, ownerName);
            ps.setBigDecimal(3, BigDecimal.valueOf(initialBalance));
            ps.executeUpdate();
        }
    }

    private static void printBalance(Connection conn, String tableName, String name) throws SQLException {
        String sql = "SELECT balance FROM " + tableName + " WHERE name = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    System.out.printf("  %s: %s%n", name, rs.getBigDecimal("balance").toPlainString());
                } else {
                    System.out.printf("  %s: Account does not exist%n", name);
                }
            }
        }
    }


    private static BigDecimal selectBalanceForUpdate(Connection conn, String tableName, int id) throws SQLException {
        String sql = "SELECT balance FROM " + tableName + " WHERE id = ? FOR UPDATE";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getBigDecimal("balance");
                } else {
                    return null;
                }
            }
        }
    }




    static class MyXid implements Xid {

        private int formatId;
        private byte[] globalTransactionId;
        private byte[] branchQualifier;

        public MyXid(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
            this.formatId = formatId;
            this.globalTransactionId = globalTransactionId;
            this.branchQualifier = branchQualifier;
        }

        @Override
        public int getFormatId() {
            return formatId;
        }

        @Override
        public byte[] getGlobalTransactionId() {
            return globalTransactionId;
        }

        @Override
        public byte[] getBranchQualifier() {
            return branchQualifier;
        }
    }


}