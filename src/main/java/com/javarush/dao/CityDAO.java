package com.javarush.dao;

import com.javarush.domain.City;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;

import java.util.List;

public class CityDAO {
    private final SessionFactory sessionFactory;

    public CityDAO(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public List<City> getItems(int offset, int limit) {
        Session session = sessionFactory.getCurrentSession();
        String hql = "SELECT c FROM City c";
        Query<City> query = session.createQuery(hql, City.class);
        query.setFirstResult(offset);
        query.setMaxResults(limit);
        return query.list();
    }

    public int getTotalCount() {
        Session session = sessionFactory.getCurrentSession();
        String hql = "SELECT COUNT(c) FROM City c";
        Query<Long> query = session.createQuery(hql, Long.class);
        return Math.toIntExact(query.getSingleResult());
    }

    public City getById(Integer id) {
        String hql = "SELECT c FROM City c JOIN FETCH c.country WHERE c.id = :id";
        Query<City> query = sessionFactory.getCurrentSession().createQuery(hql, City.class);
        query.setParameter("id", id);
        return query.getSingleResult();
    }


}
