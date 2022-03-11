package ru.seits.projects.lineardb;

import java.util.Objects;

class DataElement<T> {
    private long id;
    private long date;
    private T data;

    DataElement(long id, long date, T data) {
        this.id = id;
        this.date = date;
        this.data = data;
    }

    long getId() {
        return id;
    }

    void setId(long id) {
        this.id = id;
    }

    long getDate() {
        return date;
    }

    void setDate(long date) {
        this.date = date;
    }

    T getData() {
        return data;
    }

    void setData(T data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataElement<?> element = (DataElement<?>) o;
        return id == element.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Element{");
        sb.append("id=").append(id);
        sb.append(", date=").append(date);
        sb.append(", data=").append(data);
        sb.append('}');
        return sb.toString();
    }
}
