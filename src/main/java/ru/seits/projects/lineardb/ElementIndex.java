package ru.seits.projects.lineardb;

import java.util.List;
import java.util.Objects;

//bytes: 4+2*8
class ElementIndex implements IElement {
    private int size;
    private long position;
    private long id;
    private long date;
    private List<Object> additionalData;
    private Integer sizeInLog;
    private Long positionInLog;
    private int version;

    ElementIndex(int size, long position, long id, long date, List<Object> additionalData, Integer sizeInLog, Long positionInLog, int version) {
        this.size = size;
        this.position = position;
        this.id = id;
        this.date = date;
        this.additionalData = additionalData;
        this.sizeInLog = sizeInLog;
        this.positionInLog = positionInLog;
        this.version = version;
    }

    ElementIndex(ElementIndex elementIndex, ElementIndex elementIndexNew) {
        this(
                elementIndex != null ? elementIndex.getSize() : 0,
                elementIndex != null ? elementIndex.getPosition() : 0,
                elementIndexNew.getId(),
                elementIndexNew.getDate(),
                elementIndexNew.getAdditionalData(),
                elementIndexNew.getSizeInLog(),
                elementIndexNew.getPositionInLog(),
                elementIndexNew.version
        );
    }

    int getSize() {
        return size;
    }

    long getPosition() {
        return position;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }

    @Override
    public List<Object> getAdditionalData() {
        return additionalData;
    }

    public void setAdditionalData(List<Object> additionalData) {
        this.additionalData = additionalData;
    }

    Integer getSizeInLog() {
        return sizeInLog;
    }

    void setSizeInLog(Integer sizeInLog) {
        this.sizeInLog = sizeInLog;
    }

    Long getPositionInLog() {
        return positionInLog;
    }

    void setPositionInLog(Long positionInLog) {
        this.positionInLog = positionInLog;
    }

    @Override
    public int getVersion() {
        return version;
    }

    public int getRealSize() {
        return getSizeInLog() != null ? getSizeInLog() : getSize();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ElementIndex that = (ElementIndex) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

}
