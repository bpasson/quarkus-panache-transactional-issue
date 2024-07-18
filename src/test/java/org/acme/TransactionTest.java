package org.acme;

import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.assertj.core.api.Assertions;
import org.hibernate.internal.SessionImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

@QuarkusTest
public class TransactionTest {

    @Inject
    BookRepository bookRepository;

    @BeforeEach
    public void resetItems() {
        bookRepository.deleteAll();
    }

    // This test uses the findAll method from the base class
    // which should have a Transaction due to the class wide annotation
    @Test
    @Transactional(Transactional.TxType.NEVER)
    public void testFindAll() {

        logTransactionSession("before insert");

        // create books in a transaction
        QuarkusTransaction.joiningExisting().run(() -> {
            logTransactionSession("in transaction insert loop");
            for (int i = 0; i < 10; i++) {
                Book b = new Book();
                b.setTitle("Book " + i);
                bookRepository.persist(b);
            }
        });

        // this fetches the books and seems to store them into a
        // shared persistence context even though it should
        // not due to @Transactional on the repository.
        logTransactionSession("before listing books");
        List<Book> books = bookRepository.findAll().list();

        Assertions.assertThat(books).noneMatch(b -> b.getTitle().startsWith("Updated:"));

        // update books in transaction
        QuarkusTransaction.joiningExisting().run(() -> {
            logTransactionSession("in transaction update loop");
            bookRepository.findAll().stream().forEach(b -> {
                b.setTitle("Updated: " + b.getTitle());
            });
        });

        // this fetches the books again and I would expect it to have
        // the updated books due to @Transactional on the repository,
        // but I get the books from the shared persistence context.
        logTransactionSession("before listing books");
        List<Book> booksUpdated = bookRepository.findAll().list();

        // this fails due the managed books in the shared persistence context.
        Assertions.assertThat(booksUpdated).allMatch(b -> b.getTitle().startsWith("Updated:"), "title starts with 'Books:'");
    }

    // This test uses the findBooks method from the BooksRepository
    // which should have a Transaction due to the class wide annotation
    @Test
    @Transactional(Transactional.TxType.NEVER)
    public void testFindBooks() {

        logTransactionSession("before insert");

        // create books in a transaction
        QuarkusTransaction.joiningExisting().run(() -> {
            logTransactionSession("in transaction insert loop");
            for (int i = 0; i < 10; i++) {
                Book b = new Book();
                b.setTitle("Book " + i);
                bookRepository.persist(b);
            }
        });

        // this fetches the books and seems to store them into a
        // shared persistence context even though it should
        // not due to @Transactional on the repository.
        logTransactionSession("before listing books");
        List<Book> books = bookRepository.findAllBooks().list();

        Assertions.assertThat(books).noneMatch(b -> b.getTitle().startsWith("Updated:"));

        // update books in transaction
        QuarkusTransaction.joiningExisting().run(() -> {
            logTransactionSession("in transaction update loop");
            bookRepository.findAll().stream().forEach(b -> {
                b.setTitle("Updated: " + b.getTitle());
            });
        });

        // this fetches the books again and I would expect it to have
        // the updated books due to @Transactional on the repository,
        // but I get the books from the shared persistence context.
        logTransactionSession("before listing books");
        List<Book> booksUpdated = bookRepository.findAllBooks().list();

        // this fails due the managed books in the shared persistence context.
        Assertions.assertThat(booksUpdated).allMatch(b -> b.getTitle().startsWith("Updated:"), "title starts with 'Books:'");
    }

    // This test uses the findAll method from the base class
    // and wraps the call in a QuarkusTransaction.joiningExisting()
    @Test
    @Transactional(Transactional.TxType.NEVER)
    public void testFindAllWithQuarkusTransaction() {
        logTransactionSession("before insert");

        // create books in a transaction
        QuarkusTransaction.joiningExisting().run(() -> {
            logTransactionSession("in transaction insert loop");
            for (int i = 0; i < 10; i++) {
                Book b = new Book();
                b.setTitle("Book " + i);
                bookRepository.persist(b);
            }
        });

        // this fetches the books and does not store them in a
        // shared persistence context as expected.
        logTransactionSession("before listing books");
        List<Book> books = QuarkusTransaction.joiningExisting().call(() -> {
            logTransactionSession("inside transactional listing books");
            return bookRepository.findAll().list();
        });

        Assertions.assertThat(books).noneMatch(b -> b.getTitle().startsWith("Updated:"));

        // update books
        QuarkusTransaction.joiningExisting().run(() -> {
            logTransactionSession("in transaction update loop");
            bookRepository.findAll().stream().forEach(b -> {
                b.setTitle("Updated: " + b.getTitle());
            });
        });

        // this fetches the books again and does not store them in a
        // shared persistence context as expected.
        logTransactionSession("before listing books");
        List<Book> booksUpdated = QuarkusTransaction.joiningExisting().call(() -> {
            logTransactionSession("inside transactional listing books");
            return bookRepository.findAll().list();
        });

        // this succeeds
        Assertions.assertThat(booksUpdated).allMatch(b -> b.getTitle().startsWith("Updated:"), "title starts with 'Books:'");
    }

    // This test uses the findBooks method from the BookRepository
    // and wraps the call in a QuarkusTransaction.joiningExisting()
    @Test
    @Transactional(Transactional.TxType.NEVER)
    public void testFindBooksWithQuarkusTransaction() {
        logTransactionSession("before insert");

        // create books in a transaction
        QuarkusTransaction.joiningExisting().run(() -> {
            logTransactionSession("in transaction insert loop");
            for (int i = 0; i < 10; i++) {
                Book b = new Book();
                b.setTitle("Book " + i);
                bookRepository.persist(b);
            }
        });

        // this fetches the books and does not store them in a
        // shared persistence context as expected.
        logTransactionSession("before listing books");
        List<Book> books = QuarkusTransaction.joiningExisting().call(() -> {
            logTransactionSession("inside transactional listing books");
            return bookRepository.findAllBooks().list();
        });

        Assertions.assertThat(books).noneMatch(b -> b.getTitle().startsWith("Updated:"));

        // update books
        QuarkusTransaction.joiningExisting().run(() -> {
            logTransactionSession("in transaction update loop");
            bookRepository.findAll().stream().forEach(b -> {
                b.setTitle("Updated: " + b.getTitle());
            });
        });

        // this fetches the books again and does not store them in a
        // shared persistence context as expected.
        logTransactionSession("before listing books");
        List<Book> booksUpdated = QuarkusTransaction.joiningExisting().call(() -> {
            logTransactionSession("inside transactional listing books");
            return bookRepository.findAllBooks().list();
        });

        // this succeeds
        Assertions.assertThat(booksUpdated).allMatch(b -> b.getTitle().startsWith("Updated:"), "title starts with 'Books:'");
    }


    private void logTransactionSession(String prefix) {
        SessionImpl session = (SessionImpl) bookRepository.getEntityManager().getDelegate();
        System.out.println(prefix + ": Transaction active: " + QuarkusTransaction.isActive() + ", session=" + session +  ", books_in_context="+session.getPersistenceContext().getNumberOfManagedEntities());
    }
}
