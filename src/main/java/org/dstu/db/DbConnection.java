package org.dstu.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DbConnection {
    private static final String DB_URL = "jdbc:postgresql://localhost:5001/lab3";
    private static final String login = "postgres";
    private static final String password = "postgres";
    private static Connection connection = null;

    public static Connection getConnection() {
        if (connection == null) {
            try {
                Class.forName("org.postgresql.Driver");
                connection = DriverManager.getConnection(DB_URL, login, password);
            } catch (ClassNotFoundException | SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static Connection getNewConnection() {
        try {
            Class.forName("org.postgresql.Driver");
            return DriverManager.getConnection(DB_URL, login, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
