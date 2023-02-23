package ru.seits.projects.lineardb;

import java.util.LinkedList;
import java.util.List;

class Index {
    private long minId;
    private long minDate;
    private long maxId;
    private long maxDate;

    private LinkedList<IndexElement> elements;
    private int version;

    Index(List<IndexElement> elements, int version) {
        saveAllNew(elements);
        this.version = version;
    }

    long getMinId() {
        return minId;
    }

    void setMinId(long minId) {
        this.minId = minId;
    }

    long getMinDate() {
        return minDate;
    }

    void setMinDate(long minDate) {
        this.minDate = minDate;
    }

    long getMaxId() {
        return maxId;
    }

    void setMaxId(long maxId) {
        this.maxId = maxId;
    }

    long getMaxDate() {
        return maxDate;
    }

    void setMaxDate(long maxDate) {
        this.maxDate = maxDate;
    }

    LinkedList<IndexElement> getElements() {
        return elements;
    }

    int getVersion() {
        return version;
    }

    <T> void saveElement(DataElement<T> element, List<Object> additionalData, int size, long position) {
        IndexElement indexElement = this.getElements().stream().filter(e -> e.getId() == element.getId()).findAny().orElse(null);
        if (indexElement == null) {
            if (this.getElements().isEmpty()) {
                this.minId = element.getId();
                this.minDate = element.getDate();
                this.maxId = element.getId();
                this.maxDate = element.getDate();
            }
            updateIndexHeader(element.getId(), element.getDate());
            /*
            long nextPosition = 0;
            if (!this.getElements().isEmpty()) {
                IndexElement indexElement = this.getElements().get(this.getElements().size() - 1);
                nextPosition = indexElement.getPosition() + indexElement.getSize();
            }
            */
            this.getElements().add(new IndexElement(0, 0, element.getId(), element.getDate(), additionalData, size, position, version));
        } else {
            indexElement.setSizeInLog(size);
            indexElement.setPositionInLog(position);
            indexElement.setAdditionalData(additionalData);
            indexElement.setDate(element.getDate());
        }
    }

    public void saveAllNew(List<IndexElement> elements) {
        this.elements = elements != null ? new LinkedList<>(elements) : new LinkedList<>();
        this.minId = !this.elements.isEmpty() ? elements.get(0).getId() : 0L;
        this.minDate = !this.elements.isEmpty() ? elements.get(0).getDate() : 0L;
        this.maxId = !this.elements.isEmpty() ? elements.get(0).getId() : 0L;
        this.maxDate = !this.elements.isEmpty() ? elements.get(0).getDate() : 0L;
        updateIndex();
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

    void removeElements(int startId, int count) {
        for (int i = 0; i < count; i++)
            getElements().remove(startId);
        updateIndex();
    }

    private void updateIndex() {
        /*
        for (int i = startId; i < this.getElements().size(); i++) {
            IndexElement indexElement = this.getElements().get(i);
            indexElement.setPosition(startPosition);
            startPosition += indexElement.getSize();
        }
        */
        setMinId(getElements().stream().mapToLong(IndexElement::getId).min().orElse(0));
        setMinDate(getElements().stream().mapToLong(IndexElement::getDate).min().orElse(0));
        setMaxId(getElements().stream().mapToLong(IndexElement::getId).max().orElse(0));
        setMaxDate(getElements().stream().mapToLong(IndexElement::getDate).max().orElse(0));
    }

}
