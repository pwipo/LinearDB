package ru.seits.projects.lineardb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * small linear db
 * used for not big count of elements
 * db not have internal cache - read data operation always read file
 * full index resident in memory
 * used for log operations:
 * * add elements in end
 * * delete first n elements
 * search operations slow down as the number of items increases
 * each element mast have id (long) and date (long)
 *
 * @param <T> Element type
 */
public class DB<T> implements Closeable {

    public final static String EXTENSION_DATA = "dat";
    public final static String EXTENSION_INDEX = "idx";

    private final static int INDEX_FILE_HEADER_LENGTH = 4;
    private final static int INDEX_FILE_ELEMENT_LENGTH = 4 + 2 * 8;
    private final static int DATA_FILE_ELEMENT_HEADER_LENGTH = 4;

    private final File folder;
    private final String dbName;
    private final int version;
    private RandomAccessFile rafIndex;
    private RandomAccessFile rafData;
    private final BiFunction<Integer, byte[], T> funcConverter;
    private final BiFunction<Integer, T, byte[]> funcReverseConverter;
    private final Function<T, Long> funcGetId;
    private final BiConsumer<T, Long> funcSetId;
    private final Function<T, Long> funcGetDate;
    private final BiConsumer<T, Long> funcSetDate;

    private final int indexFileElementLength;
    private final int countAdditionalBytesInIndex;
    private final BiFunction<Integer, byte[], List<Object>> funcIndexAdditionalDataConverter;
    private final BiFunction<Integer, List<Object>, byte[]> funcIndexAdditionalDataReverseConverter;
    private final Function<T, List<Object>> funcIndexGetAdditionalData;

    private Index index;

    private int currentVersion;

    private final File indexFile;
    private final File dataFile;
    private final File indexFileTmp;
    private final File dataFileTmp;
    private final File indexFileOld;
    private final File dataFileOld;

    public DB(
            File folder
            , String dbName
            , int version
            , BiFunction<Integer, byte[], T> funcConverter
            , BiFunction<Integer, T, byte[]> funcReverseConverter
            , Function<T, Long> funcGetId
            , BiConsumer<T, Long> funcSetId
            , Function<T, Long> funcGetDate
            , BiConsumer<T, Long> funcSetDate
            , int countAdditionalBytesInIndex
            , BiFunction<Integer, byte[], List<Object>> funcIndexAdditionalDataConverter
            , BiFunction<Integer, List<Object>, byte[]> funcIndexAdditionalDataReverseConverter
            , Function<T, List<Object>> funcIndexGetAdditionalData
    ) {
        Objects.requireNonNull(folder);
        Objects.requireNonNull(dbName);
        Objects.requireNonNull(funcConverter);
        Objects.requireNonNull(funcReverseConverter);
        Objects.requireNonNull(funcGetId);
        Objects.requireNonNull(funcSetId);
        Objects.requireNonNull(funcGetDate);
        Objects.requireNonNull(funcSetDate);
        if (countAdditionalBytesInIndex > 0 &&
                (funcIndexAdditionalDataConverter == null || funcIndexAdditionalDataReverseConverter == null || funcIndexGetAdditionalData == null))
            throw new NullPointerException();
        if (countAdditionalBytesInIndex <= 0 &&
                (funcIndexAdditionalDataConverter != null || funcIndexAdditionalDataReverseConverter != null || funcIndexGetAdditionalData != null))
            throw new IllegalArgumentException();

        this.folder = folder;
        this.dbName = dbName;
        this.version = version;
        this.currentVersion = version;
        folder.mkdirs();
        this.index = null;
        this.rafIndex = null;
        this.rafData = null;
        this.funcConverter = funcConverter;
        this.funcReverseConverter = funcReverseConverter;
        this.funcGetId = funcGetId;
        this.funcSetId = funcSetId;
        this.funcGetDate = funcGetDate;
        this.funcSetDate = funcSetDate;
        this.indexFileElementLength = INDEX_FILE_ELEMENT_LENGTH + countAdditionalBytesInIndex;
        this.countAdditionalBytesInIndex = countAdditionalBytesInIndex;
        this.funcIndexAdditionalDataConverter = funcIndexAdditionalDataConverter;
        this.funcIndexAdditionalDataReverseConverter = funcIndexAdditionalDataReverseConverter;
        this.funcIndexGetAdditionalData = funcIndexGetAdditionalData;

        this.indexFile = new File(folder, dbName + "." + EXTENSION_INDEX);
        this.dataFile = new File(folder, dbName + "." + EXTENSION_DATA);
        this.indexFileTmp = new File(folder, dbName + "-tmp." + EXTENSION_INDEX);
        this.dataFileTmp = new File(folder, dbName + "-tmp." + EXTENSION_DATA);
        this.indexFileOld = new File(folder, dbName + "-old." + EXTENSION_INDEX);
        this.dataFileOld = new File(folder, dbName + "-old." + EXTENSION_DATA);
    }

    public boolean isOpen() {
        return rafIndex != null && rafData != null && index != null;
    }

    @Override
    synchronized public void close() throws IOException {
        if (rafIndex != null) {
            rafIndex.close();
            rafIndex = null;
        }
        if (rafData != null) {
            rafData.close();
            rafData = null;
        }
        index = null;
        clearTmpFiles();
    }

    synchronized public void open() throws IOException {
        if (isOpen())
            return;
        clearTmpFiles();
        if (rafIndex == null)
            this.rafIndex = new RandomAccessFile(indexFile, "rw");
        if (rafData == null)
            this.rafData = new RandomAccessFile(dataFile, "rw");
        long length = rafIndex.length();
        if (length > 0) {
            long lengthData = rafData.length();
            rafIndex.seek(0);
            currentVersion = rafIndex.readInt();
            // long minId = rafIndex.readLong();
            // long minDate = rafIndex.readLong();
            // long maxId = rafIndex.readLong();
            // long maxDate = rafIndex.readLong();

            List<IndexElement> sizes = new LinkedList<>();
            long positionInDataFile = 0;
            while (length > rafIndex.getFilePointer()) {
                if (lengthData <= positionInDataFile) {
                    rafIndex.setLength(rafIndex.getFilePointer());
                    // rafData.setLength(positionInDataFile);
                    break;
                }
                int size = rafIndex.readInt();
                long id = rafIndex.readLong();
                long date = rafIndex.readLong();
                List<Object> additionalData = null;
                if (countAdditionalBytesInIndex > 0 && funcIndexAdditionalDataConverter != null) {
                    byte[] additionalBytes = new byte[countAdditionalBytesInIndex];
                    rafIndex.readFully(additionalBytes);
                    additionalData = funcIndexAdditionalDataConverter.apply(getCurrentVersion(), additionalBytes);
                }
                sizes.add(
                        new IndexElement(
                                size
                                , positionInDataFile
                                , id
                                , date
                                , additionalData
                        ));
                positionInDataFile += size;
            }
            index = new Index(sizes);
        } else {
            index = initNewDB(rafIndex, rafData);
        }
    }

    synchronized public int getCount() {
        return index.getElements().size();
    }

    public long getMinId() {
        return index.getMinId();
    }

    public long getMaxId() {
        return index.getMaxId();
    }

    public long getMinDate() {
        return index.getMinDate();
    }

    public long getMaxDate() {
        return index.getMaxDate();
    }

    public String getDbName() {
        return dbName;
    }

    public int getVersion() {
        return version;
    }

    public int getCurrentVersion() {
        return currentVersion;
    }

    public File getFolder() {
        return folder;
    }

    synchronized public Optional<T> findById(Long id) {
        if (id < index.getMinId() || index.getMaxId() < id)
            return Optional.empty();

        return index.getElements().stream()
                .filter(e -> e.getId() == id)
                .findAny()
                .map(this::readElement);
    }

    /**
     * work as subList
     * but use ids
     *
     * @param minId low endpoint (inclusive)
     * @param maxId high endpoint (exclusive)
     * @return list of Elements
     */
    synchronized public List<T> findByIdInRange(long minId, long maxId) {
        return fastRead(findIndexElementByIdInRange(minId, maxId)
                .collect(Collectors.toList()));
    }

    /**
     * work as subList
     * but use ids
     *
     * @param minId low endpoint (inclusive)
     * @param maxId high endpoint (exclusive)
     * @return list of Element id
     */
    synchronized public List<Long> findIdsByIdInRange(long minId, long maxId) {
        return findIndexElementByIdInRange(minId, maxId)
                .map(IndexElement::getId)
                .collect(Collectors.toList());
    }

    synchronized private Stream<IndexElement> findIndexElementByIdInRange(long minId, long maxId) {
        if (minId > maxId)
            throw new IllegalArgumentException("minId should be less then maxId");
        if (minId > getMaxId() || maxId < getMinId())
            return Stream.empty();

        return index.getElements().stream()
                .filter(e -> e.getId() >= minId && e.getId() < maxId);
    }

    /**
     * work as subList
     * but use dates
     *
     * @param minDate low endpoint (inclusive)
     * @param maxDate high endpoint (exclusive)
     * @return list of Elements
     */
    synchronized public List<T> findByDateInRange(long minDate, long maxDate) {
        return fastRead(findIndexElementByDateInRange(minDate, maxDate)
                .collect(Collectors.toList()));
    }

    /**
     * work as subList
     * but use dates
     *
     * @param minDate low endpoint (inclusive)
     * @param maxDate high endpoint (exclusive)
     * @return list of Element id
     */
    synchronized public List<Long> findIdsByDateInRange(long minDate, long maxDate) {
        return findIndexElementByDateInRange(minDate, maxDate)
                .map(IndexElement::getId)
                .collect(Collectors.toList());
    }

    synchronized private Stream<IndexElement> findIndexElementByDateInRange(long minDate, long maxDate) {
        if (minDate > maxDate)
            throw new IllegalArgumentException("minDate should be less then maxDate");
        if (minDate > getMaxDate() || maxDate < getMinDate())
            return Stream.empty();

        return index.getElements().stream()
                .filter(e -> e.getDate() >= minDate && e.getDate() < maxDate);
    }

    /**
     * work as subList
     *
     * @param minPosition low endpoint (inclusive) of the subList - 0 to size of elements
     * @param maxPosition high endpoint (exclusive) of the subList - 0 to size of elements
     * @return list of Elements
     */
    synchronized public List<T> findByPositionInRange(int minPosition, int maxPosition) {
        return fastRead(findIndexElementByPositionInRange(minPosition, maxPosition).collect(Collectors.toList()));
    }

    /**
     * work as subList
     *
     * @param minPosition low endpoint (inclusive) of the subList - 0 to size of elements
     * @param maxPosition high endpoint (exclusive) of the subList - 0 to size of elements
     * @return list of Element ids
     */
    synchronized public List<Long> findIdsByPositionInRange(int minPosition, int maxPosition) {
        return findIndexElementByPositionInRange(minPosition, maxPosition)
                .map(IndexElement::getId)
                .collect(Collectors.toList());
    }

    synchronized private Stream<IndexElement> findIndexElementByPositionInRange(int minPosition, int maxPosition) {
        if (minPosition > maxPosition)
            throw new IllegalArgumentException("minPosition should be less then maxPosition");
        if (minPosition < 0)
            minPosition = 0;
        if (index.getElements().size() < maxPosition)
            maxPosition = index.getElements().size();

        return index.getElements().subList(minPosition, maxPosition).stream();
    }

    /**
     * get last N elements
     *
     * @param count last elements
     * @return list of Elements
     */
    synchronized public List<T> getLast(int count) {
        if (count <= 0)
            return new ArrayList<>();
        if (index.getElements().size() < count)
            count = index.getElements().size();

        int firstId = index.getElements().size() - count - 1;

        return fastRead(index.getElements().subList(firstId, index.getElements().size()));
    }

    synchronized private List<T> fastRead(List<IndexElement> indexElementList) {
        if (indexElementList.isEmpty())
            return new ArrayList<>();

        IndexElement min = indexElementList.stream()
                .min(Comparator.comparingLong(IndexElement::getPosition))
                .get();

        IndexElement max = indexElementList.stream()
                .max(Comparator.comparingLong(IndexElement::getPosition))
                .get();

        byte[] data = new byte[(int) (max.getPosition() + max.getSize() - min.getPosition())];
        try {
            rafData.seek(min.getPosition());
            rafData.readFully(data);
        } catch (Exception e) {
            throw new RuntimeException("error", e);
        }
        List<Long> ids = indexElementList.stream()
                .map(IndexElement::getId)
                .collect(Collectors.toList());
        return indexElementList.stream()
                .map(e -> readElement(data, (int) (e.getPosition() - min.getPosition()), e.getSize()))
                .filter(e -> ids.contains(funcGetId.apply(e)))
                .collect(Collectors.toList());
    }

    /**
     * return all elements
     *
     * @return all elements
     */
    synchronized public List<T> getAll() {
        try {
            byte[] data = new byte[(int) rafData.length()];
            rafData.seek(0);
            rafData.readFully(data);
            return index.getElements().stream()
                    .map(e -> readElement(data, (int) e.getPosition(), e.getSize()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("error", e);
        }
    }

    /**
     * return all element ids
     *
     * @return all element ids
     */
    synchronized public List<Long> getIdsAll() {
        return index.getElements().stream()
                .map(IndexElement::getId)
                .collect(Collectors.toList());
    }

    /**
     * find elements with use predicate
     * predicate get id (long) and date (long) and all additional data as list
     * fast operation, use index only
     *
     * @param filter Predicate
     * @return list of elements
     */
    synchronized public List<T> findByFilter(Predicate<List<Object>> filter) {
        return fastRead(findByFilterPrivate(filter).collect(Collectors.toList()));
    }

    /**
     * find element ids with use predicate
     * predicate get id (long) and date (long) and all additional data as list
     * fast operation, use index only
     *
     * @param filter Predicate
     * @return list of element ids
     */
    synchronized public List<Long> findIdsByFilter(Predicate<List<Object>> filter) {
        return findByFilterPrivate(filter)
                .map(IndexElement::getId)
                .collect(Collectors.toList());
    }

    synchronized private Stream<IndexElement> findByFilterPrivate(Predicate<List<Object>> filter) {
        Objects.requireNonNull(filter);
        return index.getElements().stream()
                .filter(e -> {
                    ArrayList<Object> arrayList = new ArrayList<>(e.getAdditionalData().size() + 2);
                    arrayList.add(e.getId());
                    arrayList.add(e.getDate());
                    arrayList.addAll(e.getAdditionalData());
                    return filter.test(arrayList);
                });
    }

    /**
     * find elements with use predicate
     * predicate get data element
     * very slow operation
     *
     * @param filter Predicate
     * @return list of elements
     */
    synchronized public List<T> findByDataFilter(Predicate<T> filter) {
        Objects.requireNonNull(filter);
        return index.getElements().stream()
                .map(this::readElement)
                .filter(filter)
                .collect(Collectors.toList());
    }


    /**
     * find element ids with use predicate
     * predicate get data element
     * very slow operation
     *
     * @param filter Predicate
     * @return list of element ids
     */
    synchronized public List<Long> findIdsByDataFilter(Predicate<T> filter) {
        Objects.requireNonNull(filter);
        return index.getElements().stream()
                .map(this::readElement)
                .filter(filter)
                .map(funcGetId)
                .collect(Collectors.toList());
    }

    synchronized private T readElement(IndexElement e) {
        try {
            rafData.seek(e.getPosition() + DATA_FILE_ELEMENT_HEADER_LENGTH);
            byte[] data = new byte[e.getSize() - DATA_FILE_ELEMENT_HEADER_LENGTH];
            int b = rafData.read(data);
            if (b != data.length)
                throw new Exception("wrong element size. index damaged.");
            return funcConverter.apply(getCurrentVersion(), data);
        } catch (Exception ex) {
            throw new RuntimeException("error", ex);
        }
    }

    synchronized private T readElement(byte[] dataStorage, int position, int size) {
        if (size <= DATA_FILE_ELEMENT_HEADER_LENGTH || dataStorage.length < position + size)
            throw new RuntimeException("wrong element size. index damaged.");
        try {
            byte[] data = new byte[size - DATA_FILE_ELEMENT_HEADER_LENGTH];
            System.arraycopy(dataStorage, position + DATA_FILE_ELEMENT_HEADER_LENGTH, data, 0, data.length);
            return funcConverter.apply(getCurrentVersion(), data);
        } catch (Exception ex) {
            throw new RuntimeException("error", ex);
        }
    }

    /**
     * add new data to the end
     * safe operation
     *
     * @param dataList
     * @return added elements
     * @throws IOException
     */
    synchronized public List<T> add(List<T> dataList) throws IOException {
        long oldIndexLength = rafIndex.length();
        rafIndex.seek(oldIndexLength);
        long oldDataLength = rafData.length();
        rafData.seek(oldDataLength);

        long minIdOld = index.getMinId();
        long minDateOld = index.getMinDate();
        long maxIdOld = index.getMaxId();
        long maxDateOld = index.getMaxDate();
        List<IndexElement> indexElementsOld = new ArrayList<>(index.getElements());
        try {
            return save(dataList, index, rafIndex, rafData);
        } catch (IOException e) {
            rafIndex.setLength(oldIndexLength);
            rafData.setLength(oldDataLength);

            index.setMinId(minIdOld);
            index.setMinDate(minDateOld);
            index.setMaxId(maxIdOld);
            index.setMaxDate(maxDateOld);
            index.getElements().clear();
            index.getElements().addAll(indexElementsOld);
            throw e;
        }
    }

    private Index initNewDB(RandomAccessFile rafIndex, RandomAccessFile rafData) throws IOException {
        rafIndex.seek(0);
        rafIndex.writeInt(getVersion());
        rafIndex.setLength(INDEX_FILE_HEADER_LENGTH);
        rafData.seek(0);
        rafData.setLength(0);
        currentVersion = getVersion();
        return new Index(null);
    }

    private void clearTmpFiles() {
        if (indexFileTmp.exists())
            indexFileTmp.delete();
        if (dataFileTmp.exists())
            dataFileTmp.delete();
        if (indexFileOld.exists())
            indexFileOld.delete();
        if (dataFileOld.exists())
            dataFileOld.delete();
    }

    /**
     * clear all data and save new
     * safe operation
     *
     * @param dataList new data
     * @return saved elements
     * @throws IOException
     */
    synchronized public List<T> saveAll(List<T> dataList) throws IOException {
        List<T> result;
        try {
            close();
            try (RandomAccessFile rafIndex = new RandomAccessFile(indexFileTmp, "rw"); RandomAccessFile rafData = new RandomAccessFile(dataFileTmp, "rw")) {
                Index index = initNewDB(rafIndex, rafData);
                rafIndex.seek(INDEX_FILE_HEADER_LENGTH);
                rafData.seek(0);

                result = save(dataList, index, rafIndex, rafData);
                rafIndex.setLength(rafIndex.getFilePointer());
                rafData.setLength(rafData.getFilePointer());
            }
            if (indexFile.exists() && !indexFile.renameTo(indexFileOld))
                return new ArrayList<>();
            if (dataFile.exists() && !dataFile.renameTo(dataFileOld)) {
                if (indexFileOld.exists()) {
                    indexFile.delete();
                    indexFileOld.renameTo(indexFile);
                }
                return new ArrayList<>();
            }
            if (!indexFileTmp.renameTo(indexFile) || !dataFileTmp.renameTo(dataFile)) {
                if (indexFileOld.exists()) {
                    indexFile.delete();
                    indexFileOld.renameTo(indexFile);
                }
                if (dataFileOld.exists()) {
                    dataFile.delete();
                    dataFileOld.renameTo(dataFile);
                }
            }
        } finally {
            open();
        }

        return result;
    }

    /**
     * remove elements and add to the end
     *
     * @param dataList elements for update
     * @return updated elements
     * @throws IOException
     */
    synchronized public List<T> update(List<T> dataList) throws IOException {
        List<T> result = new LinkedList<>();
        for (T element : dataList) {
            delete(funcGetId.apply(element));
            result.addAll(add(List.of(element)));
        }
        return result;
    }

    synchronized private List<T> save(List<T> dataList, Index index, RandomAccessFile rafIndex, RandomAccessFile rafData) throws IOException {
        List<T> elements = new LinkedList<>();
        long nextId = index.getMaxId() + 1;
        long date = System.currentTimeMillis();
        for (T data : dataList) {
            Long elementId = funcGetId.apply(data);
            if (elementId == null) {
                elementId = nextId++;
                funcSetId.accept(data, elementId);
            } else if (nextId < elementId) {
                nextId = elementId + 1;
            }
            Long elementDate = funcGetDate.apply(data);
            if (elementDate == null) {
                elementDate = date;
                funcSetDate.accept(data, elementDate);
            }
            byte[] bytes = funcReverseConverter.apply(getCurrentVersion(), data);
            DataElement<T> dataElement = new DataElement<>(
                    elementId,
                    elementDate,
                    data);

            int elementSize = DATA_FILE_ELEMENT_HEADER_LENGTH + bytes.length;
            rafIndex.writeInt(elementSize);
            rafIndex.writeLong(dataElement.getId());
            rafIndex.writeLong(dataElement.getDate());
            List<Object> additionalData = null;
            if (funcIndexAdditionalDataReverseConverter != null && funcIndexGetAdditionalData != null) {
                additionalData = funcIndexGetAdditionalData.apply(data);
                rafIndex.write(funcIndexAdditionalDataReverseConverter.apply(getCurrentVersion(), additionalData));
            }
            rafData.writeInt(bytes.length);
            rafData.write(bytes);

            elements.add(dataElement.getData());
            index.addElement(dataElement, additionalData, elementSize);
        }

        return elements;
    }

    /**
     * delete old elements
     * from start to first element with id greater then param
     *
     * @param id date for compare
     * @return count deleted elements
     * @throws IOException
     */
    synchronized public int deleteByIdLessThen(long id) throws IOException {
        if (getMinId() > id)
            return 0;
        Integer indexElementId = null;
        for (int i = 0; i < index.getElements().size(); i++) {
            IndexElement indexElement = index.getElements().get(i);
            if (indexElement.getId() > id)
                break;
            indexElementId = i;
        }
        if (indexElementId == null)
            return 0;

        IndexElement indexElement = index.getElements().get(indexElementId);
        removeNBytes(rafData, 0, indexElement.getPosition() + indexElement.getSize());
        removeNBytes(rafIndex, INDEX_FILE_HEADER_LENGTH, INDEX_FILE_HEADER_LENGTH + ((long) indexElementId * indexFileElementLength + indexFileElementLength));
        int count = indexElementId + 1;
        index.removeElements(0, count);

        return count;
    }

    synchronized public boolean delete(long id) throws IOException {
        if (getMinId() > id || id > getMaxId())
            return false;
        Integer indexElementId = null;
        for (int i = 0; i < index.getElements().size(); i++) {
            IndexElement indexElement = index.getElements().get(i);
            if (indexElement.getId() == id) {
                indexElementId = i;
                break;
            }
        }
        if (indexElementId == null)
            return false;

        IndexElement indexElement = index.getElements().get(indexElementId);
        removeNBytes(rafData, indexElement.getPosition(), indexElement.getPosition() + indexElement.getSize());
        removeNBytes(rafIndex, INDEX_FILE_HEADER_LENGTH + ((long) indexElementId * indexFileElementLength), INDEX_FILE_HEADER_LENGTH + ((long) indexElementId * indexFileElementLength + indexFileElementLength));
        index.removeElements(indexElementId, 1);
        return true;
    }

    synchronized private void removeNBytes(RandomAccessFile raf, long fromPosition, long toPosition) throws IOException {
        byte[] buff = new byte[1024 * 1024];
        int n;
        if (raf.length() > toPosition) {
            raf.seek(toPosition);
            while (-1 != (n = raf.read(buff))) {
                raf.seek(fromPosition);
                raf.write(buff, 0, n);
                toPosition += n;
                fromPosition += n;
                raf.seek(toPosition);
            }
        }
        raf.setLength(fromPosition);
    }

    synchronized public void removeDB() throws IOException {
        close();
        new File(folder, dbName + "." + EXTENSION_INDEX).delete();
        new File(folder, dbName + "." + EXTENSION_DATA).delete();
    }

}
