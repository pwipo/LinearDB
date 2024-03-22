package ru.seits.projects.lineardb;

import java.util.Date;
import java.util.Objects;

public class Value {
    private Long id;
    private String name;
    private String value;
    private Date version;

    public Value(Long id, String name, String value, Date version) {
        this.id = id;
        this.name = name;
        this.value = value;
        this.version = version;
    }

    public Value() {
        this(null, null, null, null);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Date getVersion() {
        return version;
    }

    public void setVersion(Date version) {
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Value that = (Value) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
