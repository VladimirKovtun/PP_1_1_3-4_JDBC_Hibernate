package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UserDaoHibernateImpl implements UserDao {

    private final Logger logger = Logger.getLogger(UserDaoHibernateImpl.class.getName());
    private final SessionFactory sessionFactory = Util.getSessionFactory();
    public UserDaoHibernateImpl() {

    }

    @Override
    public void createUsersTable() {
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();
            session.createSQLQuery("CREATE TABLE IF NOT EXISTS users " +
                    "(id INTEGER not NULL AUTO_INCREMENT, " +
                    " name VARCHAR(50) not NULL, " +
                    " last_name VARCHAR (50) not NULL, " +
                    " age INTEGER not NULL, " +
                    " PRIMARY KEY (id))").executeUpdate();
            session.getTransaction().commit();
            logger.log(Level.INFO, "Table create.");
        } catch (Exception e) {
            sessionFactory.getCurrentSession().getTransaction().rollback();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropUsersTable() {
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();
            session.createSQLQuery("DROP TABLE IF EXISTS users").executeUpdate();
            session.getTransaction().commit();
            logger.log(Level.INFO, " Table drop.");
        } catch (Exception e) {
            sessionFactory.getCurrentSession().getTransaction().rollback();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();
            Serializable saveId = session.save(new User(name, lastName, age));
            session.getTransaction().commit();
            logger.log(Level.INFO, "Save User {0}", saveId);
        } catch (Exception e) {
            sessionFactory.getCurrentSession().getTransaction().rollback();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeUserById(long id) {
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();
            session.delete(session.get(User.class, id));
            session.getTransaction().commit();
            logger.log(Level.INFO, "Remove User");
        } catch (Exception e) {
            sessionFactory.getCurrentSession().getTransaction().rollback();
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<User> getAllUsers() {
        List<User> userList = new ArrayList<>();
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();
            userList = session.createQuery("from User", User.class).list();
            session.getTransaction().commit();
            logger.log(Level.INFO, "List users {0}", userList);
        } catch (Exception e) {
            sessionFactory.getCurrentSession().getTransaction().rollback();
            throw new RuntimeException(e);
        }
        return userList;
    }

    @Override
    public void cleanUsersTable() {
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();
            session.createQuery("delete from User").executeUpdate();
            session.getTransaction().commit();
            logger.log(Level.INFO, "Clear table.");
        } catch (Exception e) {
            sessionFactory.getCurrentSession().getTransaction().rollback();
            throw new RuntimeException(e);
        }
    }
}
