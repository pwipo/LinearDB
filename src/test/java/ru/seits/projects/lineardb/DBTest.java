package ru.seits.projects.lineardb;


import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DBTest {

    private static DB<ElementTest> db;

    @BeforeAll
    public static void onlyOnce() throws IOException {
        db = new DB<>(
                new File("linerdb"),
                "db",
                1,
                ElementTest::fromByte,
                ElementTest::toByte,
                ElementTest::getId,
                ElementTest::setId,
                ElementTest::getDate,
                ElementTest::setDate,
                null,
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
        List<ElementTest> elements = db.save(List.of(
                new ElementTest(idGenerator.incrementAndGet(), 1, 2, currentTimeMillis)
                , new ElementTest(idGenerator.incrementAndGet(), 3, 4, currentTimeMillis)
                , new ElementTest(null, 5, 6, currentTimeMillis)
        ));
        Assertions.assertEquals(3, elements.size());
        elements.forEach(System.out::println);
    }

    @Test
    @Order(2)
    public void add() throws IOException {
        List<ElementTest> elements = db.save(List.of(
                new ElementTest(null, 7, 8, null)
        ));
        Assertions.assertEquals(1, elements.size());
        elements.forEach(System.out::println);
    }

    @Test
    @Order(3)
    public void getAll() {
        List<ElementTest> all = db.getAll();
        Assertions.assertTrue(all.size() >= 4);
        all.forEach(System.out::println);
    }

    @Test
    @Order(4)
    public void getById() {
        Optional<ElementTest> byId = db.findById(1L);
        Assertions.assertTrue(byId.isPresent());
        byId.ifPresent(System.out::println);
    }

    @Test
    @Order(5)
    public void findByIdInRange() {
        List<ElementTest> elements = db.findByIdInRange(db.getMinId(), db.getMaxId());
        Assertions.assertFalse(elements.isEmpty());
        Assertions.assertTrue(elements.size() > 2);
        elements.forEach(System.out::println);
    }

    @Test
    @Order(6)
    public void findByDateInRange() {
        List<ElementTest> elements = db.findByDateInRange(db.getMinDate(), Math.max(db.getMaxDate(), db.getMinDate() + 1));
        Assertions.assertFalse(elements.isEmpty());
        elements.forEach(System.out::println);
    }

    @Test
    @Order(7)
    public void findByPositionInRange() {
        List<ElementTest> elements = db.findByPositionInRange(1, 3);
        Assertions.assertFalse(elements.isEmpty());
        elements.forEach(System.out::println);
    }

    @Test
    @Order(8)
    public void delete() throws IOException {
        long maxId = db.getMaxId();
        List<ElementTest> elements = db.save(List.of(
                new ElementTest(maxId + 1, 7, 8, System.currentTimeMillis())
        ));
        Assertions.assertEquals(1, elements.size());
        elements.forEach(System.out::println);

        boolean delete = db.delete(maxId);
        Assertions.assertTrue(delete);

        List<ElementTest> all = db.getAll();
        Assertions.assertTrue(all.size() >= 3);
        all.forEach(System.out::println);
    }

    @Test
    @Order(9)
    public void deleteOld() throws IOException {
        long minDate = db.getMinDate();
        long maxDate = db.getMaxDate();
        List<Long> idsByDateInRange = db.findIdsByDateInRange(minDate, minDate + 1);
        Assertions.assertTrue(idsByDateInRange.size() >= 1);
        Integer count = db.deleteByIdLessThen(idsByDateInRange.get(idsByDateInRange.size() - 1));
        Assertions.assertTrue(count >= 1);

        List<ElementTest> all = db.getAll();
        Assertions.assertTrue(maxDate - minDate < 1000 || all.size() >= 1);
        all.forEach(System.out::println);
    }

}