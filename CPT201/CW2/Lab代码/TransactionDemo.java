/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
//package xjtlu.cpt201.transaction;

/**
 *
 * @author weiwang-mac2
 */
import java.math.BigDecimal;
import java.sql.*;

public class TransactionDemo {

    // TODO: adjust these for your environment
    private static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/transactiondb?useSSL=false&serverTimezone=UTC";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "02390239Caoqi";

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS acco" +
                    "unts ("
                    + "id INT PRIMARY KEY,"
                    + "name VARCHAR(100) NOT NULL,"
                    + "balance DECIMAL(19,4) NOT NULL"
                    + ") ENGINE=InnoDB";

    private static final String DELETE_DEMO_SQL = "DELETE FROM accounts WHERE id IN (1,2)";
    private static final String INSERT_ACCOUNT_SQL = "INSERT INTO accounts (id, name, balance) VALUES (?, ?, ?)";
    private static final String SELECT_ALL_SQL = "SELECT id, name, balance FROM accounts ORDER BY id";
    private static final String SELECT_FOR_UPDATE_SQL = "SELECT balance FROM accounts WHERE id = ? FOR UPDATE";
    private static final String UPDATE_BALANCE_SQL = "UPDATE accounts SET balance = ? WHERE id = ?";

    public static void main(String[] args) {
        try {
            // Optional: explicit driver load (usually not necessary with modern JDBC)
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
            } catch (ClassNotFoundException ignored) {}

            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
                System.out.println("Connected to database: " + DB_URL);

                initSchema(conn);
                seedDemoData(conn);

                System.out.println("\nInitial balances:");
                printAllBalances(conn);

                System.out.println("\n--- Successful transfer (commit) ---");
                performTransferWithValidationAndRollback(conn, 1, 2, new BigDecimal("250.00"));
                printAllBalances(conn);

                System.out.println("\n--- Transfer that should not be allowed ---");
                performTransferWithValidationAndRollback(conn, 1, 2, new BigDecimal("2000.00"));
                printAllBalances(conn);

                System.out.println("\nDemo finished.");
            }
        } catch (SQLException ex) {
            System.err.println("Database error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }


    private static boolean validateTransfer(Connection conn, int fromId, int toId, BigDecimal amount) throws SQLException {
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            System.out.println("  Validation failed: Amount must be positive");
            return false;
        }

        BigDecimal fromBal = selectBalanceForUpdate(conn, fromId);
        if (fromBal.compareTo(amount) < 0) {
            System.out.println("  Validation failed: Insufficient balance. Available: " + fromBal.toPlainString() + ", Requested: " + amount.toPlainString());
            return false;
        }

        System.out.println("  Validation passed: Transfer of " + amount.toPlainString() + " from account " + fromId + " to account " + toId);
        return true;
    }


    private static void initSchema(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(CREATE_TABLE_SQL);
        }
    }

    private static void seedDemoData(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate(DELETE_DEMO_SQL);
        }
        try (PreparedStatement ps = conn.prepareStatement(INSERT_ACCOUNT_SQL)) {
            ps.setInt(1, 1);
            ps.setString(2, "Alice");
            ps.setBigDecimal(3, new BigDecimal("1000.00"));
            ps.executeUpdate();

            ps.setInt(1, 2);
            ps.setString(2, "Bob");
            ps.setBigDecimal(3, new BigDecimal("500.00"));
            ps.executeUpdate();
        }
    }

    private static void printAllBalances(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(SELECT_ALL_SQL)) {
            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                BigDecimal bal = rs.getBigDecimal("balance");
                System.out.printf("  %d: %s = %s%n", id, name, bal.toPlainString());
            }
        }
    }

    // Simple transfer: uses transaction and commits when all is ok
    private static void performTransfer(Connection conn, int fromId, int toId, BigDecimal amount) {
        try {
            conn.setAutoCommit(false);

            BigDecimal fromBal = selectBalanceForUpdate(conn, fromId);
            BigDecimal toBal = selectBalanceForUpdate(conn, toId);

            if (fromBal == null || toBal == null) {
                throw new SQLException("Account not found.");
            }

            System.out.printf("  Before: from=%s, to=%s%n", fromBal.toPlainString(), toBal.toPlainString());

            BigDecimal newFrom = fromBal.subtract(amount);
            BigDecimal newTo = toBal.add(amount);

            updateBalance(conn, fromId, newFrom);
            updateBalance(conn, toId, newTo);

            conn.commit();
            System.out.println("  Transfer committed.");
        } catch (SQLException ex) {
            safeRollback(conn);
            System.out.println("  Transfer rolled back: " + ex.getMessage());
        } finally {
            safeResetAutoCommit(conn);
        }
    }

    private static BigDecimal selectBalanceForUpdate(Connection conn, int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SELECT_FOR_UPDATE_SQL)) {
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

    private static void updateBalance(Connection conn, int id, BigDecimal newBalance) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(UPDATE_BALANCE_SQL)) {
            ps.setBigDecimal(1, newBalance);
            ps.setInt(2, id);
            int updated = ps.executeUpdate();
            if (updated != 1) {
                throw new SQLException("Expected to update 1 row for id=" + id + " but updated " + updated);
            }
        }
    }

    private static void safeRollback(Connection conn) {
        if (conn == null) return;
        try {
            conn.rollback();
        } catch (SQLException e) {
            System.err.println("  Failed to rollback: " + e.getMessage());
        }
    }

    private static void safeResetAutoCommit(Connection conn) {
        if (conn == null) return;
        try {
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            System.err.println("  Failed to reset autoCommit: " + e.getMessage());
        }
    }

    private static void performTransferWithValidationAndRollback(Connection conn, int fromId, int toId, BigDecimal amount){
        try {
            conn.setAutoCommit(false);

            if (!validateTransfer(conn, fromId, toId, amount)) {
                System.out.println("  Transfer failed validation and was rolled back.");
                return;
            }

            BigDecimal fromBal = selectBalanceForUpdate(conn, fromId);
            BigDecimal toBal = selectBalanceForUpdate(conn, toId);

            if (fromBal == null || toBal == null) {
                throw new SQLException("Account not found.");
            }

            System.out.printf("  Before: from=%s, to=%s%n", fromBal.toPlainString(), toBal.toPlainString());

            BigDecimal newFrom = fromBal.subtract(amount);
            BigDecimal newTo = toBal.add(amount);

            updateBalance(conn, fromId, newFrom);
            updateBalance(conn, toId, newTo);

            conn.commit();
            System.out.println("  Transfer committed.");
        } catch (SQLException ex) {
            safeRollback(conn);
            System.out.println("  Transfer rolled back: " + ex.getMessage());
        } finally {
            safeResetAutoCommit(conn);
        }

    }
}