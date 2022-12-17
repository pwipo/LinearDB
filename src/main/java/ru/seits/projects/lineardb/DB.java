package ru.seits.projects.lineardb;

import java.io.*;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * small linear db
 * has log file
 * used for not big count of elements
 * db not have internal cache - read data operation always read file
 * full index resident in memory
 * used log file for operations. it increase speed:
 * * add elements
 * * update elements
 * * delete first n elements
 * * delete element
 * search operations slow down as the number of items increases
 * each element mast have id (long) and date (long)
 *
 * @param <T> Element type
 */
public class DB<T> implements Closeable {

    public final static String EXTENSION_DATA = "dat";
    public final static String EXTENSION_INDEX = "idx";
    public final static String EXTENSION_LOG = "log";
    public final static String EXTENSION_LOCK = "lck";

    private final static int INDEX_FILE_HEADER_LENGTH = 4;
    private final static int INDEX_FILE_ELEMENT_LENGTH = 4 + 2 * 8;
    private final static int DATA_FILE_HEADER_LENGTH = 4;
    private final static int DATA_FILE_ELEMENT_HEADER_LENGTH = 4;
    private final static int LOG_FILE_HEADER_LENGTH = 4;
    private final static int LOG_FILE_ELEMENT_HEADER_LENGTH = 1 + INDEX_FILE_ELEMENT_LENGTH;

    private final static byte LOG_ELEMENT_TYPE_SAVE = 1;
    private final static byte LOG_ELEMENT_TYPE_DELETE = 2;

    private final File folder;
    private final String dbName;
    private final int version;
    private RandomAccessFile rafIndex;
    private RandomAccessFile rafData;
    private RandomAccessFile rafLog;
    private RandomAccessFile rafLock;
    private final BiFunction<Integer, byte[], T> funcConverter;
    private final BiFunction<Integer, T, byte[]> funcReverseConverter;
    private final Function<T, Long> funcGetId;
    private final BiConsumer<T, Long> funcSetId;
    private final Function<T, Long> funcGetDate;
    private final BiConsumer<T, Long> funcSetDate;

    private final int indexFileElementLength;
    private final int countAdditionalBytesInIndex;
    private final int logFileElementHeaderLength;
    private final BiFunction<Integer, byte[], List<Object>> funcIndexAdditionalDataConverter;
    private final BiFunction<Integer, List<Object>, byte[]> funcIndexAdditionalDataReverseConverter;
    private final Function<T, List<Object>> funcIndexGetAdditionalData;

    private Index index;

    private final File indexFile;
    private final File dataFile;
    private final File logFile;
    private final File lockFile;
    private final File indexFileNew;
    private final File dataFileNew;
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
        this.logFileElementHeaderLength = LOG_FILE_ELEMENT_HEADER_LENGTH + countAdditionalBytesInIndex;
        this.funcIndexAdditionalDataConverter = funcIndexAdditionalDataConverter;
        this.funcIndexAdditionalDataReverseConverter = funcIndexAdditionalDataReverseConverter;
        this.funcIndexGetAdditionalData = funcIndexGetAdditionalData;

        this.indexFile = new File(folder, dbName + "." + EXTENSION_INDEX);
        this.dataFile = new File(folder, dbName + "." + EXTENSION_DATA);
        this.logFile = new File(folder, dbName + "." + EXTENSION_LOG);
        this.lockFile = new File(folder, dbName + "." + EXTENSION_LOCK);
        this.indexFileNew = new File(folder, dbName + "-new." + EXTENSION_INDEX);
        this.dataFileNew = new File(folder, dbName + "-new." + EXTENSION_DATA);
        this.indexFileOld = new File(folder, dbName + "-old." + EXTENSION_INDEX);
        this.dataFileOld = new File(folder, dbName + "-old." + EXTENSION_DATA);

        if (lockFile.exists()) {
            try {
                if (!lockFile.delete())
                    throw new IllegalAccessError("db in use");
            } catch (Exception e) {
                throw new IllegalAccessError("db in use");
            }
        }
    }

    public boolean isOpen() {
        return rafIndex != null && rafData != null && index != null && rafLog != null;
    }

    @Override
    synchronized public void close() throws IOException {
        close(true);
    }

    synchronized public void close(boolean needApplyLog) throws IOException {
        if (needApplyLog)
            applyLog(index, false);
        if (rafLog != null) {
            rafLog.close();
            rafLog = null;
        }
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
        if (rafLock != null) {
            rafLock.close();
            rafLock = null;
        }
        if (lockFile.exists())
            lockFile.delete();
    }

    synchronized public void open() throws IOException {
        if (isOpen())
            return;
        clearTmpFiles();
        if (!lockFile.exists()) {
            try {
                lockFile.createNewFile();
                lockFile.deleteOnExit();
            } catch (Exception e) {
                throw new RuntimeException("error while create lock file", e);
            }
        }
        if (rafIndex == null)
            this.rafIndex = new RandomAccessFile(indexFile, "rw");
        if (rafData == null)
            this.rafData = new RandomAccessFile(dataFile, "rw");
        if (rafLog == null)
            this.rafLog = new RandomAccessFile(logFile, "rw");
        if (rafLock == null)
            this.rafLock = new RandomAccessFile(lockFile, "r");
        long length = rafIndex.length();
        long lengthData = rafData.length();
        if (length > 0 && lengthData > 0) {
            rafIndex.seek(0);
            int versionIndex = rafIndex.readInt();
            // long minId = rafIndex.readLong();
            // long minDate = rafIndex.readLong();
            // long maxId = rafIndex.readLong();
            // long maxDate = rafIndex.readLong();

            List<IndexElement> sizes = new LinkedList<>();
            long positionInDataFile = DATA_FILE_HEADER_LENGTH;
            while (length > rafIndex.getFilePointer()) {
                if (lengthData <= positionInDataFile) {
                    rafIndex.setLength(rafIndex.getFilePointer());
                    // rafData.setLength(positionInDataFile);
                    break;
                }
                IndexElement indexElement = readIndexElement(rafIndex, positionInDataFile, countAdditionalBytesInIndex, versionIndex);
                sizes.add(indexElement);
                positionInDataFile += indexElement.getSize();
            }
            index = new Index(sizes, versionIndex);
        } else {
            index = initNewDB(rafIndex, rafData);
        }
        if (rafLog.length() > LOG_FILE_HEADER_LENGTH) {
            applyLog(buildIndexFromLog(), true);
        } else {
            initLog(rafLog);
        }
    }

    synchronized private IndexElement readIndexElement(RandomAccessFile raf, long position, int countAdditionalBytesInIndex, int ver) throws IOException {
        int size = raf.readInt();
        long id = raf.readLong();
        long date = raf.readLong();
        List<Object> additionalData = null;
        if (countAdditionalBytesInIndex > 0 && funcIndexAdditionalDataConverter != null) {
            byte[] additionalBytes = new byte[countAdditionalBytesInIndex];
            raf.readFully(additionalBytes);
            additionalData = funcIndexAdditionalDataConverter.apply(ver, additionalBytes);
        }
        return new IndexElement(
                size
                , position > -1 ? position : rafLog.getFilePointer()
                , id
                , date
                , additionalData
                , null
                , null
                , ver
        );
    }

    synchronized private Index buildIndexFromLog() throws IOException {
        int ver = rafLog.readInt();
        long length = rafLog.length();
        Map<Long, IndexElement> lastInLog = new HashMap<>();
        while (length > rafLog.getFilePointer()) {
            byte type = rafLog.readByte();
            IndexElement indexElement = null;
            if (LOG_ELEMENT_TYPE_SAVE == type) {
                indexElement = readIndexElement(rafLog, -1, countAdditionalBytesInIndex, ver);
                indexElement.setPositionInLog(indexElement.getPosition());
                indexElement.setSizeInLog(indexElement.getSize());
                if (indexElement.getSize() > 0) {
                    // byte[] data = new byte[size];
                    // rafLog.readFully(data);
                    /*int countSkipped = */
                    rafLog.skipBytes(indexElement.getSize());
                }
                lastInLog.put(indexElement.getId(), indexElement);
            } else if (LOG_ELEMENT_TYPE_DELETE == type) {
                indexElement = readIndexElement(rafLog, 0, 0, ver);
                lastInLog.put(indexElement.getId(), null);
            }
        }
        List<IndexElement> indexElements = new LinkedList<>();

        //for update and remove
        for (IndexElement indexElement : index.getElements()) {
            if (lastInLog.containsKey(indexElement.getId())) {
                IndexElement indexElementNew = lastInLog.get(indexElement.getId());
                if (indexElementNew == null)
                    continue;
                indexElements.add(new IndexElement(indexElement, indexElementNew));
            } else {
                indexElements.add(indexElement);
            }
        }

        //for create
        Map<Long, IndexElement> idsInIndex = index.getElements().stream().collect(Collectors.toMap(IndexElement::getId, id -> id));
        lastInLog.values().forEach(i -> {
            if (i == null)
                return;
            if (idsInIndex.containsKey(i.getId()))
                return;
            indexElements.add(new IndexElement(null, i));
        });

        return new Index(indexElements, getVersion());
    }

    synchronized private void applyLog(Index index, boolean reinit) throws IOException {
        if (!isOpen() || rafLog.length() == 0)
            return;
        clearTmpFiles();
        try (RandomAccessFile rafIndexNew = new RandomAccessFile(indexFileNew, "rw"); RandomAccessFile rafDataNew = new RandomAccessFile(dataFileNew, "rw")) {
            initNewDB(rafIndexNew, rafDataNew);
            LinkedList<IndexElement> indexElements = new LinkedList<>();
            for (IndexElement indexElement : index.getElements()) {
                try {
                    RandomAccessFile rafIn = indexElement.getPositionInLog() != null ? rafLog : rafData;
                    long position = indexElement.getPositionInLog() != null ? indexElement.getPositionInLog() : indexElement.getPosition();
                    int size = indexElement.getSizeInLog() != null ? indexElement.getSizeInLog() : indexElement.getSize();
                    byte[] data = new byte[size - DATA_FILE_ELEMENT_HEADER_LENGTH];
                    rafIn.seek(position + DATA_FILE_ELEMENT_HEADER_LENGTH);
                    rafIn.readFully(data);
                    if (indexElement.getVersion() != getVersion()) {
                        T obj = funcConverter.apply(indexElement.getVersion(), data);
                        if (obj != null)
                            data = funcReverseConverter.apply(getVersion(), obj);
                        if (data == null)
                            continue;
                    }

                    writeElementDirect(rafIndexNew, rafDataNew, data.length + DATA_FILE_ELEMENT_HEADER_LENGTH, indexElement.getId(), indexElement.getDate(), indexElement.getAdditionalData(), data, indexElement.getVersion());
                    indexElements.add(indexElement);
                } catch (Exception e) {
                    //if error - not stop operation
                    e.printStackTrace();
                }
            }
            index.saveAllNew(indexElements);
        }

        boolean rafIndexOpen = rafIndex != null;
        boolean rafDataOpen = rafData != null;

        if (rafIndexOpen)
            rafIndex.close();
        if (indexFile.exists() && !indexFile.renameTo(indexFileOld)) {
            if (rafIndexOpen)
                this.rafIndex = new RandomAccessFile(indexFile, "rw");
            return;
        }
        if (rafDataOpen)
            rafData.close();
        if (dataFile.exists() && !dataFile.renameTo(dataFileOld)) {
            if (indexFileOld.exists()) {
                indexFile.delete();
                indexFileOld.renameTo(indexFile);
            }
            if (rafIndexOpen)
                this.rafIndex = new RandomAccessFile(indexFile, "rw");
            if (rafDataOpen)
                this.rafData = new RandomAccessFile(dataFile, "rw");
            return;
        }
        if (!indexFileNew.renameTo(indexFile) || !dataFileNew.renameTo(dataFile)) {
            if (indexFileOld.exists()) {
                indexFile.delete();
                indexFileOld.renameTo(indexFile);
            }
            if (dataFileOld.exists()) {
                dataFile.delete();
                dataFileOld.renameTo(dataFile);
            }
        }
        if (rafIndexOpen)
            this.rafIndex = new RandomAccessFile(indexFile, "rw");
        if (rafDataOpen)
            this.rafData = new RandomAccessFile(dataFile, "rw");

        initLog(rafLog);
        if (reinit) {
            close(false);
            open();
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
                .filter(e -> e.getPositionInLog() == null && e.getSizeInLog() == null)
                .min(Comparator.comparingLong(IndexElement::getPosition))
                .orElse(null);
        IndexElement max = indexElementList.stream()
                .filter(e -> e.getPositionInLog() == null && e.getSizeInLog() == null)
                .max(Comparator.comparingLong(IndexElement::getPosition))
                .orElse(null);
        byte[] data = null;
        if (min != null && max != null) {
            data = new byte[(int) (max.getPosition() + max.getSize() - min.getPosition())];
            try {
                rafData.seek(min.getPosition());
                rafData.readFully(data);
            } catch (Exception e) {
                throw new RuntimeException("error", e);
            }
        }

        List<Long> ids = indexElementList.stream()
                .map(IndexElement::getId)
                .collect(Collectors.toList());
        byte[] dataTmp = data;
        return indexElementList.stream()
                .map(e -> {
                    if (dataTmp != null && e.getPositionInLog() == null && e.getSizeInLog() == null) {
                        return readElement(dataTmp, (int) (e.getPosition() - min.getPosition()), e.getSize(), e.getVersion());
                    } else {
                        return readElement(e);
                    }
                })
                .filter(Objects::nonNull)
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
            /*
            byte[] data = new byte[(int) rafData.length()];
            rafData.seek(0);
            rafData.readFully(data);
            return index.getElements().stream()
                    .map(e -> readElement(data, (int) e.getPosition(), e.getSize()))
                    .collect(Collectors.toList());
            */
            return fastRead(index.getElements());
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
                .filter(Objects::nonNull)
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
                .filter(Objects::nonNull)
                .filter(filter)
                .map(funcGetId)
                .collect(Collectors.toList());
    }

    synchronized private T readElement(IndexElement e) {
        try {
            RandomAccessFile rafIn = e.getPositionInLog() != null ? rafLog : rafData;
            long position = e.getPositionInLog() != null ? e.getPositionInLog() : e.getPosition();
            int size = e.getSizeInLog() != null ? e.getSizeInLog() : e.getSize();
            rafIn.seek(position + DATA_FILE_ELEMENT_HEADER_LENGTH);
            byte[] data = new byte[size - DATA_FILE_ELEMENT_HEADER_LENGTH];
            int b = rafIn.read(data);
            if (b != data.length)
                throw new Exception("wrong element size. index damaged.");
            return funcConverter.apply(e.getPositionInLog() != null ? getVersion() : e.getVersion(), data);
        } catch (Exception ex) {
            // throw new RuntimeException("error", ex);
            ex.printStackTrace();
            return null;
        }
    }

    synchronized private T readElement(byte[] dataStorage, int position, int size, int version) {
        try {
            if (size <= DATA_FILE_ELEMENT_HEADER_LENGTH || dataStorage.length < position + size)
                throw new RuntimeException("wrong element size. index damaged.");
            byte[] data = new byte[size - DATA_FILE_ELEMENT_HEADER_LENGTH];
            System.arraycopy(dataStorage, position + DATA_FILE_ELEMENT_HEADER_LENGTH, data, 0, data.length);
            return funcConverter.apply(version, data);
        } catch (Exception ex) {
            // throw new RuntimeException("error", ex);
            ex.printStackTrace();
            return null;
        }
    }

    /**
     * add new data or update exest
     * write data to the end of log
     * safe operation
     *
     * @param dataList
     * @return added elements
     * @throws IOException
     */
    synchronized public List<T> save(List<T> dataList) throws IOException {
        if (dataList == null || dataList.isEmpty())
            return dataList;
        // long oldIndexLength = rafIndex.length();
        // rafIndex.seek(oldIndexLength);
        // long oldDataLength = rafData.length();
        // rafData.seek(oldDataLength);
        long oldLogLength = rafLog.length();
        // rafLog.seek(oldLogLength);

        long minIdOld = index.getMinId();
        long minDateOld = index.getMinDate();
        long maxIdOld = index.getMaxId();
        long maxDateOld = index.getMaxDate();
        List<IndexElement> indexElementsOld = new ArrayList<>(index.getElements());
        try {
            return operationSave(dataList);
        } catch (IOException e) {
            // rafIndex.setLength(oldIndexLength);
            // rafData.setLength(oldDataLength);
            rafLog.setLength(oldLogLength);

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
        rafData.writeInt(getVersion());
        rafData.setLength(DATA_FILE_HEADER_LENGTH);
        return new Index(null, getVersion());
    }

    private void initLog(RandomAccessFile rafLog) throws IOException {
        rafLog.seek(0);
        rafLog.writeInt(getVersion());
        rafLog.setLength(LOG_FILE_HEADER_LENGTH);
    }

    private void clearTmpFiles() {
        if (indexFileNew.exists())
            indexFileNew.delete();
        if (dataFileNew.exists())
            dataFileNew.delete();
        if (indexFileOld.exists())
            indexFileOld.delete();
        if (dataFileOld.exists())
            dataFileOld.delete();
    }

    /*
     * clear all data and save new
     * safe operation
     *
     * @param dataList new data
     * @return saved elements
     * @throws IOException
     */
    // synchronized public List<T> saveAll(List<T> dataList) throws IOException {
    //     List<T> result;
    //     try {
    //         close();
    //         try (RandomAccessFile rafIndex = new RandomAccessFile(indexFileTmp, "rw"); RandomAccessFile rafData = new RandomAccessFile(dataFileTmp, "rw")) {
    //             Index index = initNewDB(rafIndex, rafData);
    //             rafIndex.seek(INDEX_FILE_HEADER_LENGTH);
    //             rafData.seek(0);
    //
    //             result = save(dataList, index, rafIndex, rafData);
    //             rafIndex.setLength(rafIndex.getFilePointer());
    //             rafData.setLength(rafData.getFilePointer());
    //         }
    //         if (indexFile.exists() && !indexFile.renameTo(indexFileOld))
    //             return new ArrayList<>();
    //         if (dataFile.exists() && !dataFile.renameTo(dataFileOld)) {
    //             if (indexFileOld.exists()) {
    //                 indexFile.delete();
    //                 indexFileOld.renameTo(indexFile);
    //             }
    //             return new ArrayList<>();
    //         }
    //         if (!indexFileTmp.renameTo(indexFile) || !dataFileTmp.renameTo(dataFile)) {
    //             if (indexFileOld.exists()) {
    //                 indexFile.delete();
    //                 indexFileOld.renameTo(indexFile);
    //             }
    //             if (dataFileOld.exists()) {
    //                 dataFile.delete();
    //                 dataFileOld.renameTo(dataFile);
    //             }
    //         }
    //     } finally {
    //         open();
    //     }
    //
    //     return result;
    // }

    /*
     * remove elements and add to the end
     *
     * @param dataList elements for update
     * @return updated elements
     * @throws IOException
     */
    /*
    synchronized public List<T> update(List<T> dataList) throws IOException {
        List<T> result = new LinkedList<>();
        for (T element : dataList) {
            delete(funcGetId.apply(element));
            result.addAll(add(List.of(element)));
        }
        return result;
    }
    */

    synchronized private List<T> operationSave(List<T> dataList) throws IOException {
        List<T> elements = new LinkedList<>();
        long nextId = index.getMaxId() + 1;
        long date = System.currentTimeMillis();
        long position = rafLog.getFilePointer();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos)) {
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

                byte[] bytes = funcReverseConverter.apply(getVersion(), data);
                DataElement<T> dataElement = new DataElement<>(
                        elementId,
                        elementDate,
                        data);

                int elementSize = DATA_FILE_ELEMENT_HEADER_LENGTH + bytes.length;
                List<Object> additionalData = null;
                if (funcIndexAdditionalDataReverseConverter != null && funcIndexGetAdditionalData != null)
                    additionalData = funcIndexGetAdditionalData.apply(data);
                writeElementToLog(dos, LOG_ELEMENT_TYPE_SAVE, elementSize, dataElement.getId(), dataElement.getDate(), additionalData, bytes);

                elements.add(dataElement.getData());
                position += logFileElementHeaderLength;
                index.saveElement(dataElement, additionalData, elementSize, position);
                position += elementSize;
            }
            dos.flush();
            rafLog.write(baos.toByteArray());
            rafLog.setLength(rafLog.getFilePointer());
        }

        return elements;
    }

    synchronized private void operationDelete(int startPosition, int count) throws IOException {
        List<IndexElement> indexElements = index.getElements().subList(startPosition, startPosition + count);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos)) {
            for (IndexElement indexElement : indexElements) {
                writeElementToLog(dos, LOG_ELEMENT_TYPE_DELETE, 0, indexElement.getId(), System.currentTimeMillis(), null, null);
            }
            dos.flush();
            rafLog.write(baos.toByteArray());
            rafLog.setLength(rafLog.getFilePointer());
        }
        index.removeElements(startPosition, count);
    }

    synchronized private void writeElementDirect(RandomAccessFile rafIndex, RandomAccessFile rafData, int elementSize, long id, long date, List<Object> additionalData, byte[] bytes, int version) throws IOException {
        //check consistent
        if (funcConverter.apply(getVersion(), bytes) == null)
            throw new IllegalArgumentException("wrong data");
        byte[] additional = null;
        if (additionalData != null)
            additional = funcIndexAdditionalDataReverseConverter.apply(getVersion(), additionalData);
        rafData.writeInt(bytes.length);
        rafData.write(bytes);
        rafIndex.writeInt(elementSize);
        rafIndex.writeLong(id);
        rafIndex.writeLong(date);
        if (additional != null)
            rafIndex.write(additional);
    }

    synchronized private void writeElementToLog(DataOutputStream dos, byte type, int elementSize, long id, long date, List<Object> additionalData, byte[] bytes) throws IOException {
        //check consistent
        if (bytes != null && funcConverter.apply(getVersion(), bytes) == null)
            throw new IllegalArgumentException("wrong data");
        byte[] additional = null;
        if (additionalData != null)
            additional = funcIndexAdditionalDataReverseConverter.apply(getVersion(), additionalData);
        dos.writeByte(type);
        dos.writeInt(elementSize);
        dos.writeLong(id);
        dos.writeLong(date);
        if (additional != null)
            dos.write(additional);
        if (bytes != null) {
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }
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

        // IndexElement indexElement = index.getElements().get(indexElementId);
        // removeNBytes(rafData, 0, indexElement.getPosition() + indexElement.getSize());
        // removeNBytes(rafIndex, INDEX_FILE_HEADER_LENGTH, INDEX_FILE_HEADER_LENGTH + ((long) indexElementId * indexFileElementLength + indexFileElementLength));
        int count = indexElementId + 1;
        operationDelete(0, count);
        // index.removeElements(0, count);

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

        // IndexElement indexElement = index.getElements().get(indexElementId);
        // removeNBytes(rafData, indexElement.getPosition(), indexElement.getPosition() + indexElement.getSize());
        // removeNBytes(rafIndex, INDEX_FILE_HEADER_LENGTH + ((long) indexElementId * indexFileElementLength), INDEX_FILE_HEADER_LENGTH + ((long) indexElementId * indexFileElementLength + indexFileElementLength));
        operationDelete(indexElementId, 1);
        // index.removeElements(indexElementId, 1);
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
        close(false);
        new File(folder, dbName + "." + EXTENSION_INDEX).delete();
        new File(folder, dbName + "." + EXTENSION_DATA).delete();
        new File(folder, dbName + "." + EXTENSION_LOG).delete();
    }

}
