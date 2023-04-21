package com.javarush.dao;

import com.javarush.domain.Country;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;

import java.util.List;

public class CountryDAO {
    private final SessionFactory sessionFactory;

    public CountryDAO(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public List<Country> getALL() {
        Session session = sessionFactory.getCurrentSession();
        String hql = "SELECT c FROM Country c JOIN FETCH c.languages";
        Query<Country> query = session.createQuery(hql, Country.class);
        return query.list();
    }
}
