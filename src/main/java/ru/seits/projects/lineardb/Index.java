package ru.seits.projects.lineardb;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

class Index {
    private long minId;
    private long minDate;
    private long maxId;
    private long maxDate;

    private LinkedList<ElementIndex> elements;
    private final int version;

    Index(List<ElementIndex> elements, int version) {
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

    LinkedList<ElementIndex> getElements() {
        return elements;
    }

    int getVersion() {
        return version;
    }

    Optional<ElementIndex> findOne(Long id) {
        return this.elements.stream().filter(e -> e.getId() == id).findAny();
    }

    <T> void saveElement(ElementData<T> element, List<Object> additionalData, int size, long position) {
        ElementIndex elementIndex = findOne(element.getId()).orElse(null);
        if (elementIndex == null) {
            if (this.elements.isEmpty() || this.getMinId() > element.getId())
                this.setMaxId(element.getId());
            if (this.elements.isEmpty() || this.getMinDate() > element.getDate())
                this.setMinDate(element.getDate());
            if (this.elements.isEmpty() || this.getMaxId() < element.getId())
                this.setMaxId(element.getId());
            if (this.elements.isEmpty() || this.getMaxDate() < element.getDate())
                this.setMaxDate(element.getDate());
            /*
            long nextPosition = 0;
            if (!this.getElements().isEmpty()) {
                IndexElement indexElement = this.getElements().get(this.getElements().size() - 1);
                nextPosition = indexElement.getPosition() + indexElement.getSize();
            }
            */
            this.elements.add(new ElementIndex(0, 0, element.getId(), element.getDate(), additionalData, size, position, version));
        } else {
            elementIndex.setSizeInLog(size);
            elementIndex.setPositionInLog(position);
            elementIndex.setAdditionalData(additionalData);
            elementIndex.setDate(element.getDate());
            if (this.getMinDate() > element.getDate())
                this.setMinDate(element.getDate());
            if (this.getMaxDate() < element.getDate())
                this.setMaxDate(element.getDate());
        }
    }

    public void saveAllNew(List<ElementIndex> elements) {
        this.elements = elements != null ? new LinkedList<>(elements) : new LinkedList<>();
        setMinId(!this.elements.isEmpty() ? elements.stream().mapToLong(ElementIndex::getId).min().orElse(0) : 0L);
        setMinDate(!this.elements.isEmpty() ? elements.stream().mapToLong(ElementIndex::getDate).min().orElse(0) : 0L);
        setMaxId(!this.elements.isEmpty() ? elements.stream().mapToLong(ElementIndex::getId).max().orElse(0) : 0L);
        setMaxDate(!this.elements.isEmpty() ? elements.stream().mapToLong(ElementIndex::getDate).max().orElse(0) : 0L);
    }

    void removeElements(List<ElementIndex> elements) {
        elements.forEach(this.elements::remove);
        if (getMinId() == elements.stream().mapToLong(ElementIndex::getId).min().orElse(0))
            setMinId(this.elements.stream().mapToLong(ElementIndex::getId).min().orElse(0));
        if (getMinDate() == elements.stream().mapToLong(ElementIndex::getDate).min().orElse(0))
            setMinDate(this.elements.stream().mapToLong(ElementIndex::getDate).min().orElse(0));
        if (getMaxId() == elements.stream().mapToLong(ElementIndex::getId).max().orElse(0))
            setMaxId(this.elements.stream().mapToLong(ElementIndex::getId).max().orElse(0));
        if (getMaxDate() == elements.stream().mapToLong(ElementIndex::getDate).max().orElse(0))
            setMaxDate(this.elements.stream().mapToLong(ElementIndex::getDate).max().orElse(0));
    }

}
