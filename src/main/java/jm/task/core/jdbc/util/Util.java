package jm.task.core.jdbc.util;

import jm.task.core.jdbc.model.User;
import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.service.ServiceRegistry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Util {
   private static final Logger LOGGER = Logger.getLogger(Util.class.getName());
   private static final String USERNAME = "root";
   private static final String PASSWORD = "1111";
   private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
   private static final String connectionUrl = "jdbc:mysql://localhost:3306/test";
   private static Connection connection;
   private static SessionFactory sessionFactory;
   private Util(){

   }

   public static SessionFactory getSessionFactory() {
      if (sessionFactory == null) {
         try {
            Configuration configuration = new Configuration();
            Properties properties = new Properties();

            properties.put(Environment.DRIVER, DRIVER);
            properties.put(Environment.URL, connectionUrl);
            properties.put(Environment.USER, USERNAME);
            properties.put(Environment.PASS, PASSWORD);
            properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQLDialect");
            properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
            properties.put(Environment.SHOW_SQL, "true");
            properties.put(Environment.HBM2DDL_AUTO, "none");

            configuration.setProperties(properties);
            configuration.addAnnotatedClass(User.class);

            ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder().
                    applySettings(configuration.getProperties()).build();
            sessionFactory = configuration.buildSessionFactory(serviceRegistry);
         } catch (HibernateException e) {
            LOGGER.log(Level.SEVERE, "Problem creating session factory.");
            throw new RuntimeException(e);
         }
      }
      return sessionFactory;
   }

   public static Connection getConnection() {
      if (connection == null) {
         try {
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(connectionUrl, USERNAME, PASSWORD);
         } catch (ClassNotFoundException | SQLException e) {
            LOGGER.log(Level.SEVERE, "Connection failed!!");
            throw new RuntimeException(e);
         }
      }
      return connection;
   }
}
