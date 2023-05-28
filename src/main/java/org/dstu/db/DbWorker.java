package org.dstu.db;

import org.dstu.util.CsvReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class DbWorker {
    public static void populateFromFile(String fileName) {
        List<String[]> strings = CsvReader.readCsvFile(fileName, ";");
        Connection conn = DbConnection.getConnection();
        try {
            Statement cleaner = conn.createStatement();
            System.out.println(cleaner.executeUpdate("DELETE FROM bed"));
            System.out.println(cleaner.executeUpdate("DELETE FROM sofa"));
            PreparedStatement badSt = conn.prepareStatement(
                    "INSERT INTO bed (manufacturer, color, price, size, have_headboard, mattress_type) " +
                            "VALUES (?, ?, ?, ?, ?, ?)");
            PreparedStatement sofaSt = conn.prepareStatement(
                    "INSERT INTO sofa (manufacturer, color, price, count_of_seats, upfolding_mechanizm, have_armrests) " +
                            "VALUES (?, ?, ?, ?, ?, ?)");

            for (String[] line: strings) {
                if (line[0].equals("0")) {
                    badSt.setString(1, line[1]);
                    badSt.setString(2, line[2]);
                    badSt.setInt(3, Integer.parseInt(line[3]));
                    badSt.setString(4, line[4]);
                    badSt.setBoolean(5, Boolean.parseBoolean(line[5]));
                    badSt.setString(6, line[6]);
                    badSt.addBatch();
                } else {
                    sofaSt.setString(1, line[1]);
                    sofaSt.setString(2, line[2]);
                    sofaSt.setInt(3, Integer.parseInt(line[3]));
                    sofaSt.setInt(4, Integer.parseInt(line[4]));
                    sofaSt.setString(5, line[5]);
                    sofaSt.setBoolean(6, Boolean.parseBoolean(line[6]));
                    sofaSt.addBatch();
                }
            }
            int[] badRes = badSt.executeBatch();
            int[] sofaRes = sofaSt.executeBatch();
            for (int num: badRes) {
                System.out.println(num);
            }

            for (int num: sofaRes) {
                System.out.println(num);
            }
            cleaner.close();
            badSt.close();
            sofaSt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void demoQuery() {
        Connection conn = DbConnection.getConnection();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT manufacturer, color, price FROM bed WHERE price > 1900");
            while (rs.next()) {
                System.out.print(rs.getString("manufacturer"));
                System.out.print(" ");
                System.out.print(rs.getString("color"));
                System.out.print(" ");
                System.out.println(rs.getString("price"));
            }
            rs.close();
            st.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void dirtyReadDemo() {
        Runnable first = () -> {
            Connection conn1 = DbConnection.getNewConnection();
            if (conn1 != null) {
                try {
                    conn1.setAutoCommit(false);
                    conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement upd = conn1.createStatement();
                    upd.executeUpdate("UPDATE sofa SET color='Красный' WHERE price > 1300");
                    Thread.sleep(2000);
                    conn1.rollback();
                    upd.close();
                    Statement st = conn1.createStatement();
                    System.out.println("In the first thread:");
                    ResultSet rs = st.executeQuery("SELECT * FROM sofa");
                    while (rs.next()) {
                        System.out.println(rs.getString("color"));
                    }
                    st.close();
                    rs.close();
                    conn1.close();
                } catch (SQLException | InterruptedException throwables) {
                    throwables.printStackTrace();
                }
            }
        };

        Runnable second = () -> {
            Connection conn2 = DbConnection.getNewConnection();
            if (conn2 != null) {
                try {
                    Thread.sleep(500);
                    conn2.setAutoCommit(false);
                    conn2.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement st = conn2.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM sofa");
                    while (rs.next()) {
                        System.out.println(rs.getString("color"));
                    }
                    rs.close();
                    st.close();
                    conn2.close();
                } catch (SQLException | InterruptedException throwables) {
                    throwables.printStackTrace();
                }
            }
        };
        Thread th1 = new Thread(first);
        Thread th2 = new Thread(second);
        th1.start();
        th2.start();
    }
}
