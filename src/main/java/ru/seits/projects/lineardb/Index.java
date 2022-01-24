package ru.seits.projects.lineardb;

import java.util.LinkedList;
import java.util.List;

class Index {
    private long minId;
    private long minDate;
    private long maxId;
    private long maxDate;

    private LinkedList<IndexElement> elements;

    public Index(List<IndexElement> elements) {
        this.elements = elements != null ? new LinkedList<>(elements) : new LinkedList<>();

        this.minId = !this.elements.isEmpty() ? elements.get(0).getId() : 0L;
        this.minDate = !this.elements.isEmpty() ? elements.get(0).getDate() : 0L;
        this.maxId = !this.elements.isEmpty() ? elements.get(0).getId() : 0L;
        this.maxDate = !this.elements.isEmpty() ? elements.get(0).getDate() : 0L;
        this.elements.forEach(e -> updateIndexHeader(e.getId(), e.getDate()));
    }

    public long getMinId() {
        return minId;
    }

    public void setMinId(long minId) {
        this.minId = minId;
    }

    public long getMinDate() {
        return minDate;
    }

    public void setMinDate(long minDate) {
        this.minDate = minDate;
    }

    public long getMaxId() {
        return maxId;
    }

    public void setMaxId(long maxId) {
        this.maxId = maxId;
    }

    public long getMaxDate() {
        return maxDate;
    }

    public void setMaxDate(long maxDate) {
        this.maxDate = maxDate;
    }

    public LinkedList<IndexElement> getElements() {
        return elements;
    }

    public <T> void addElement(DataElement<T> element, List<Object> additionalData, int size) {
        if (this.getElements().isEmpty()) {
            this.minId = element.getId();
            this.minDate = element.getDate();
            this.maxId = element.getId();
            this.maxDate = element.getDate();
        }
        updateIndexHeader(element.getId(), element.getDate());
        long nextPosition = 0;
        if (!this.getElements().isEmpty()) {
            IndexElement indexElement = this.getElements().get(this.getElements().size() - 1);
            nextPosition = indexElement.getPosition() + indexElement.getSize();
        }
        this.getElements().add(new IndexElement(size, nextPosition, element.getId(), element.getDate(), additionalData));
    }

    private void updateIndexHeader(long id, long date) {
        if (this.getMinId() > id)
            this.minId = id;
        if (this.getMinDate() > date)
            this.minDate = date;
        if (this.getMaxId() < id)
            this.maxId = id;
        if (this.getMaxDate() < date)
            this.maxDate = date;
    }

    public void removeElements(int startId, int count) {
        long positionInDataFile = this.getElements().get(startId).getPosition();
        for (int i = 0; i < count; i++)
            getElements().remove(startId);
        updateIndex(startId, positionInDataFile);
    }

    private void updateIndex(int startId, long startPosition) {
        for (int i = startId; i < this.getElements().size(); i++) {
            IndexElement indexElement = this.getElements().get(i);
            indexElement.setPosition(startPosition);
            startPosition += indexElement.getSize();
        }

        setMinId(getElements().stream().mapToLong(IndexElement::getId).min().orElse(0));
        setMinDate(getElements().stream().mapToLong(IndexElement::getDate).min().orElse(0));
        setMaxId(getElements().stream().mapToLong(IndexElement::getId).max().orElse(0));
        setMaxDate(getElements().stream().mapToLong(IndexElement::getDate).max().orElse(0));
    }

}
