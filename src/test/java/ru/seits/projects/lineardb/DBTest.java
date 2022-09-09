package ru.seits.projects.lineardb;


import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DBTest {

    private static DB<TestElement> db;

    @BeforeAll
    public static void onlyOnce() throws IOException {
        db = new DB<>(
                new File("linerdb"),
                "db",
                1,
                TestElement::fromByte,
                TestElement::toByte,
                TestElement::getId,
                TestElement::setId,
                TestElement::getDate,
                TestElement::setDate,
                0,
                null,
                null,
                null
        );
        db.open();
    }

    @AfterAll
    public static void onStop() throws IOException {
        db.close();
    }

    @Test
    @Order(0)
    public void version() {
        Assertions.assertEquals(1, db.getVersion());
    }

    @Test
    @Order(1)
    public void saveAll() throws IOException {
        AtomicLong idGenerator = new AtomicLong(0);
        long currentTimeMillis = System.currentTimeMillis();
        List<TestElement> elements = db.save(List.of(
                new TestElement(idGenerator.incrementAndGet(), 1, 2, currentTimeMillis)
                , new TestElement(idGenerator.incrementAndGet(), 3, 4, currentTimeMillis)
                , new TestElement(null, 5, 6, currentTimeMillis)
        ));
        Assertions.assertEquals(3, elements.size());
        elements.forEach(System.out::println);
    }

    @Test
    @Order(2)
    public void add() throws IOException {
        List<TestElement> elements = db.save(List.of(
                new TestElement(null, 7, 8, null)
        ));
        Assertions.assertEquals(1, elements.size());
        elements.forEach(System.out::println);
    }

    @Test
    @Order(3)
    public void getAll() {
        List<TestElement> all = db.getAll();
        Assertions.assertTrue(all.size() >= 4);
        all.forEach(System.out::println);
    }

    @Test
    @Order(4)
    public void getById() {
        Optional<TestElement> byId = db.findById(1L);
        Assertions.assertTrue(byId.isPresent());
        byId.ifPresent(System.out::println);
    }

    @Test
    @Order(5)
    public void findByIdInRange() {
        List<TestElement> elements = db.findByIdInRange(db.getMinId(), db.getMinId() + 2);
        Assertions.assertFalse(elements.isEmpty());
        Assertions.assertEquals(2, elements.size());
        elements.forEach(System.out::println);
    }

    @Test
    @Order(6)
    public void findByDateInRange() {
        List<TestElement> elements = db.findByDateInRange(db.getMinDate(), db.getMaxDate());
        Assertions.assertFalse(elements.isEmpty());
        Assertions.assertTrue(elements.size() >= 3);
        elements.forEach(System.out::println);
    }

    @Test
    @Order(7)
    public void findByPositionInRange() {
        List<TestElement> elements = db.findByPositionInRange(1, 3);
        Assertions.assertFalse(elements.isEmpty());
        elements.forEach(System.out::println);
    }

    @Test
    @Order(8)
    public void delete() throws IOException {
        long maxId = db.getMaxId();
        List<TestElement> elements = db.save(List.of(
                new TestElement(maxId + 1, 7, 8, System.currentTimeMillis())
        ));
        Assertions.assertEquals(1, elements.size());
        elements.forEach(System.out::println);

        boolean delete = db.delete(maxId);
        Assertions.assertTrue(delete);

        List<TestElement> all = db.getAll();
        Assertions.assertTrue(all.size() >= 4);
        all.forEach(System.out::println);
    }

    @Test
    @Order(9)
    public void deleteOld() throws IOException {
        List<Long> idsByDateInRange = db.findIdsByDateInRange(db.getMinDate(), db.getMinDate() + 1);
        Assertions.assertTrue(idsByDateInRange.size() >= 1);
        Integer count = db.deleteByIdLessThen(idsByDateInRange.get(idsByDateInRange.size() - 1));
        Assertions.assertTrue(count >= 1);

        List<TestElement> all = db.getAll();
        Assertions.assertTrue(all.size() >= 1);
        all.forEach(System.out::println);
    }

}