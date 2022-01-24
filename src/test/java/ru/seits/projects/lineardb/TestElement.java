package ru.seits.projects.lineardb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Objects;

public class TestElement {
    private Long id;
    private int x;
    private int y;
    private Long date;

    public TestElement(Long id, int x, int y, Long date) {
        this.id = id;
        this.x = x;
        this.y = y;
        this.date = date;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TestElement{");
        sb.append("id=").append(id);
        sb.append(", x=").append(x);
        sb.append(", y=").append(y);
        sb.append(", date=").append(date);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestElement that = (TestElement) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public static TestElement fromByte(byte[] bytes) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes); DataInputStream dis = new DataInputStream(bais)) {
            return new TestElement(dis.readLong(), dis.readInt(), dis.readInt(), dis.readLong());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] toByte(TestElement obj) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(obj.id);
            dos.writeInt(obj.x);
            dos.writeInt(obj.y);
            dos.writeLong(obj.date);
            dos.flush();
            baos.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public Long getDate() {
        return date;
    }

    public void setDate(Long date) {
        this.date = date;
    }
}
