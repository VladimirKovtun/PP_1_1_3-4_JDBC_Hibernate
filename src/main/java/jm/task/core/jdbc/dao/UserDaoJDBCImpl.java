package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UserDaoJDBCImpl implements UserDao {
    private final Logger LOGGER = Logger.getLogger(UserDaoJDBCImpl.class.getName());
    private final Connection connection = Util.getConnection();

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        try (PreparedStatement preparedStatement = connection.prepareStatement("CREATE TABLE IF NOT EXISTS users " +
                "(id INTEGER not NULL AUTO_INCREMENT, " +
                " name VARCHAR(50) not NULL, " +
                " last_name VARCHAR (50) not NULL, " +
                " age INTEGER not NULL, " +
                " PRIMARY KEY (id))")) {
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            rollbackQuietly(connection);
            LOGGER.log(Level.SEVERE, "Create table failed!!");
            throw new RuntimeException(e);
        }
    }

    public void dropUsersTable() {
        try (PreparedStatement preparedStatement = connection.prepareStatement("DROP TABLE IF EXISTS `users`")) {
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            rollbackQuietly(connection);
            LOGGER.log(Level.SEVERE, "Drop table failed!");
            throw new RuntimeException(e);
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (PreparedStatement preparedStatement =
                     connection.prepareStatement("INSERT INTO users (name, last_name, age) VALUES (?, ?, ?)")) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();
            connection.commit();
            LOGGER.log(Level.INFO, "User c {0} добавлен в базу", name);
        } catch (SQLException e) {
            rollbackQuietly(connection);
            LOGGER.log(Level.SEVERE, "Save user failed!");
            throw new RuntimeException(e);
        }
    }

    public void removeUserById(long id) {
        try (PreparedStatement preparedStatement =
                     connection.prepareStatement("DELETE FROM users where id = ?")) {

            preparedStatement.setLong(1, id);
            int row = preparedStatement.executeUpdate();
            connection.commit();
            LOGGER.log(Level.INFO, "{0} rows deleted", row);
        } catch (SQLException e) {
            rollbackQuietly(connection);
            LOGGER.log(Level.SEVERE, "Delete user failed!");
            throw new RuntimeException(e);
        }
    }

    public List<User> getAllUsers() {
        List<User> usersList = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM users")) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("last_name"));
                user.setAge(resultSet.getByte("age"));
                usersList.add(user);
            }
            connection.commit();
            LOGGER.log(Level.INFO, "{0} ", usersList);
        } catch (SQLException e) {
            rollbackQuietly(connection);
            LOGGER.log(Level.SEVERE, "Fail!");
            throw new RuntimeException(e);
        }
        return usersList;
    }

    public void cleanUsersTable() {
        try (PreparedStatement preparedStatement = connection.prepareStatement("TRUNCATE TABLE users")) {
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            rollbackQuietly(connection);
            LOGGER.log(Level.SEVERE, "Clean table failed!");
            throw new RuntimeException(e);
        }
    }

    private void rollbackQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
