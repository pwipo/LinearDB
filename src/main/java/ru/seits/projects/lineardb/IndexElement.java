package ru.seits.projects.lineardb;

import java.util.List;

//bytes: 4+2*8
class IndexElement {
    private int size;
    private long position;
    private long id;
    private long date;
    private List<Object> additionalData;

    public IndexElement(int size, long position, long id, long date, List<Object> additionalData) {
        this.size = size;
        this.position = position;
        this.id = id;
        this.date = date;
        this.additionalData = additionalData;
    }

    public int getSize() {
        return size;
    }

    public long getPosition() {
        return position;
    }

    void setPosition(long position) {
        this.position = position;
    }

    public long getId() {
        return id;
    }

    public long getDate() {
        return date;
    }

    public List<Object> getAdditionalData() {
        return additionalData;
    }
}
