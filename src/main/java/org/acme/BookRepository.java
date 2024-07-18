package org.acme;

import io.quarkus.hibernate.orm.panache.PanacheQuery;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.hibernate.internal.SessionImpl;

// annotated with @Transactional to force all methods to either have or begin a transaction
@ApplicationScoped
@Transactional
public class BookRepository implements PanacheRepositoryBase<Book, Long> {

    public PanacheQuery<Book> findAllBooks() {
        logTransactionSession("in findAllBooks()");
        return findAll();
    }

    private void logTransactionSession(String prefix) {
        SessionImpl session = (SessionImpl) getEntityManager().getDelegate();
        System.out.println(prefix + ": Transaction active: " + QuarkusTransaction.isActive() + ", session=" + session +  ", books_in_context="+session.getPersistenceContext().getNumberOfManagedEntities());
    }
}
