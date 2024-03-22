package ru.seits.projects.lineardb;


import org.junit.jupiter.api.*;

import java.io.*;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DBTestStringKey {
    private static DB<Value> db;
    private static final int DB_VER = 1;
    private static final String DB_INDEX_NAME_FIELD = String.join("", Collections.nCopies(10, " "));
    private static final int DB_INDEX_NAME_LENGTH = DB_INDEX_NAME_FIELD.length();
    private static final int DB_INDEX_NAME_BYTES_LENGTH = DB_INDEX_NAME_FIELD.getBytes().length;

    @BeforeAll
    public static void onlyOnce() throws IOException {
        db = new DB<>(
                new File("linerdb2"),
                "db",
                DB_VER,
                DBTestStringKey::fromByte,
                DBTestStringKey::toByte,
                Value::getId,
                Value::setId,
                o -> o.getVersion() != null ? o.getVersion().getTime() : null,
                (o, time) -> o.setVersion(new Date(time)),
                (ver) -> ver > 0 ? DB_INDEX_NAME_BYTES_LENGTH : 0,
                (ver, b) -> ver > 0 ? List.of(new String(b)) : List.of(),
                (ver, list) -> {
                    if (ver <= 0)
                        return null;
                    byte[] bytes = new byte[DB_INDEX_NAME_BYTES_LENGTH];
                    byte[] arr1 = list.get(0).toString().getBytes();
                    System.arraycopy(arr1, 0, bytes, 0, arr1.length);
                    return bytes;
                },
                (ver, c) -> ver > 0 && c.getName() != null ? List.of(c.getName().length() > DB_INDEX_NAME_LENGTH ? c.getName().substring(0, DB_INDEX_NAME_LENGTH) : c.getName()) : null
        );
        db.open();
    }

    private static byte[] toByte(Integer ver, Value c) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(c.getId());
            dos.writeUTF(c.getName());
            dos.writeUTF(c.getValue() != null ? c.getValue() : "");
            dos.writeLong(c.getVersion().getTime());
            dos.flush();
            baos.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("toByte", e);
        }
    }

    private static Value fromByte(Integer ver, byte[] bytes) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes); DataInputStream dis = new DataInputStream(bais)) {
            Value obj = new Value();
            obj.setId(dis.readLong());
            obj.setName(dis.readUTF());
            obj.setValue(dis.readUTF());
            obj.setVersion(new Date(dis.readLong()));
            return obj;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
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
        Date date = new Date();
        List<Value> elements = db.save(List.of(
                new Value(idGenerator.incrementAndGet(), "one", "akljhl", date)
                , new Value(idGenerator.incrementAndGet(), "two", "adtjtdlo;", date)
                , new Value(null, "three_long_key", "54sdxghjkfy56", date)
        ));
        Assertions.assertEquals(3, elements.size());
        elements.forEach(System.out::println);
    }

    @Test
    @Order(2)
    public void add() throws IOException {
        List<Value> elements = db.save(List.of(
                new Value(null, "four", "dsp[oiqpewrb", null)
        ));
        Assertions.assertEquals(1, elements.size());
        elements.forEach(System.out::println);
    }

    @Test
    @Order(3)
    public void getAll() {
        List<Value> all = db.getAll();
        Assertions.assertTrue(all.size() >= 4);
        all.forEach(System.out::println);
    }

    @Test
    @Order(4)
    public void getById() {
        Optional<Value> byId = db.findById(1L);
        Assertions.assertTrue(byId.isPresent());
        byId.ifPresent(System.out::println);
    }

    @Test
    @Order(5)
    public void findByIdInRange() {
        List<Value> elements = db.findByIdInRange(db.getMinId(), db.getMaxId());
        Assertions.assertFalse(elements.isEmpty());
        Assertions.assertTrue(elements.size() > 2);
        elements.forEach(System.out::println);
    }

    @Test
    @Order(6)
    public void findByDateInRange() {
        List<Value> elements = db.findByDateInRange(db.getMinDate(), db.getMaxDate());
        Assertions.assertFalse(elements.isEmpty());
        Assertions.assertTrue(elements.size() >= 3);
        elements.forEach(System.out::println);
    }

    @Test
    @Order(7)
    public void findByPositionInRange() {
        List<Value> elements = db.findByPositionInRange(1, 3);
        Assertions.assertFalse(elements.isEmpty());
        elements.forEach(System.out::println);
    }

    @Test
    @Order(8)
    public void findByName() {
        List<Value> values = db.findOneByStringIndexKey("one", 0);
        Assertions.assertFalse(values.isEmpty());
        System.out.println(values.get(0));
        values = db.findOneByStringIndexKey("two", 0);
        Assertions.assertFalse(values.isEmpty());
        System.out.println(values.get(0));
        values = db.findOneByStringIndexKey("three_long_key", 0);
        Assertions.assertTrue(values.isEmpty());
        values = db.findOneByStringIndexKey("three_long_key", 0, DB_INDEX_NAME_LENGTH, Value::getName);
        Assertions.assertFalse(values.isEmpty());
        System.out.println(values.get(0));
    }

    @Test
    @Order(9)
    public void delete() throws IOException {
        long maxId = db.getMaxId();
        List<Value> elements = db.save(List.of(
                new Value(maxId + 1, "five", "alkdjnlgfds[rs", null)
        ));
        Assertions.assertEquals(1, elements.size());
        elements.forEach(System.out::println);

        boolean delete = db.delete(maxId);
        Assertions.assertTrue(delete);

        List<Value> all = db.getAll();
        Assertions.assertTrue(all.size() >= 3);
        all.forEach(System.out::println);
    }

    @Test
    @Order(10)
    public void deleteOld() throws IOException {
        long minDate = db.getMinDate();
        long maxDate = db.getMaxDate();
        List<Long> idsByDateInRange = db.findIdsByDateInRange(minDate, minDate + 1);
        Assertions.assertFalse(idsByDateInRange.isEmpty());
        int count = db.deleteByIdLessThen(idsByDateInRange.get(idsByDateInRange.size() - 1));
        Assertions.assertTrue(count >= 1);

        List<Value> all = db.getAll();
        Assertions.assertTrue(maxDate - minDate < 1000 || !all.isEmpty());
        all.forEach(System.out::println);
    }

}