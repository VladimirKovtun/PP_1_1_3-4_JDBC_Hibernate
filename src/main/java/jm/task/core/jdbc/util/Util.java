package jm.task.core.jdbc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Util {
    private static final Logger LOGGER = Logger.getLogger(Util.class.getName());
    private static final String USERNAME = "root";
    private static final String PASSWORD = "1111";
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String connectionUrl = "jdbc:mysql://localhost:3306/test";

    public static Connection getConnection() {
        try {
            Class.forName(JDBC_DRIVER);
            Connection connection = DriverManager.getConnection(connectionUrl, USERNAME, PASSWORD);
            connection.setAutoCommit(false);
            return connection;
        } catch (ClassNotFoundException | SQLException e) {
            LOGGER.log(Level.SEVERE, "Connection failed!!");
            throw new RuntimeException(e);
        }
    }
}
