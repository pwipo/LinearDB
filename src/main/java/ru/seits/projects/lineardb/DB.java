package ru.seits.projects.lineardb;

import java.io.*;
import java.util.*;
import java.util.function.*;
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
    public final static int LOG_FILE_ELEMENT_HEADER_LENGTH = 1 + INDEX_FILE_ELEMENT_LENGTH;
    private final static int DATA_FILE_HEADER_LENGTH = 4;
    private final static int DATA_FILE_ELEMENT_HEADER_LENGTH = 4;
    private final static int LOG_FILE_HEADER_LENGTH = 4;
    private final static byte LOG_ELEMENT_TYPE_SAVE = 1;
    private final static byte LOG_ELEMENT_TYPE_DELETE = 2;

    private final File folder;
    private final String dbName;
    private final int version;
    private final IObjectBuilder<T> funcConverter;
    private final IBytesBuilder<T> funcReverseConverter;
    private final Function<T, Long> funcGetId;
    private final BiConsumer<T, Long> funcSetId;
    private final Function<T, Long> funcGetDate;
    private final BiConsumer<T, Long> funcSetDate;
    private final Function<Integer, Integer> funcCountAdditionalBytes;
    private final BiFunction<Integer, byte[], List<Object>> funcIndexAdditionalDataConverter;
    private final BiFunction<Integer, List<Object>, byte[]> funcIndexAdditionalDataReverseConverter;
    private final BiFunction<Integer, T, List<Object>> funcIndexGetAdditionalData;
    private final File indexFile;
    private final File dataFile;
    private final File logFile;
    private final File lockFile;
    private final File indexFileNew;
    private final File dataFileNew;
    private final File indexFileOld;
    private final File dataFileOld;
    private RandomAccessFile rafIndex;
    private RandomAccessFile rafData;
    private RandomAccessFile rafLog;
    private RandomAccessFile rafLock;
    // private final int countAdditionalBytes;
    // private final int indexFileElementLength;
    private int logFileElementHeaderLength;
    private Index index;

    public DB(
            File folder
            , String dbName
            , int version
            , IObjectBuilder<T> funcConverter
            , IBytesBuilder<T> funcReverseConverter
            , Function<T, Long> funcGetId
            , BiConsumer<T, Long> funcSetId
            , Function<T, Long> funcGetDate
            , BiConsumer<T, Long> funcSetDate
            , Function<Integer, Integer> funcCountAdditionalBytes
            , BiFunction<Integer, byte[], List<Object>> funcIndexAdditionalDataConverter
            , BiFunction<Integer, List<Object>, byte[]> funcIndexAdditionalDataReverseConverter
            , BiFunction<Integer, T, List<Object>> funcIndexGetAdditionalData
    ) {
        Objects.requireNonNull(folder);
        Objects.requireNonNull(dbName);
        Objects.requireNonNull(funcConverter);
        Objects.requireNonNull(funcReverseConverter);
        Objects.requireNonNull(funcGetId);
        Objects.requireNonNull(funcSetId);
        Objects.requireNonNull(funcGetDate);
        Objects.requireNonNull(funcSetDate);
        if (funcCountAdditionalBytes != null &&
                (funcIndexAdditionalDataConverter == null || funcIndexAdditionalDataReverseConverter == null || funcIndexGetAdditionalData == null))
            throw new NullPointerException();
        if (funcCountAdditionalBytes == null &&
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
        int countAdditionalBytes = funcCountAdditionalBytes != null ? funcCountAdditionalBytes.apply(version) : 0;
        // this.indexFileElementLength = INDEX_FILE_ELEMENT_LENGTH + countAdditionalBytes;
        this.logFileElementHeaderLength = LOG_FILE_ELEMENT_HEADER_LENGTH + countAdditionalBytes;
        this.funcCountAdditionalBytes = funcCountAdditionalBytes;
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
            int countAdditionalBytesIndex = funcCountAdditionalBytes != null ? funcCountAdditionalBytes.apply(versionIndex) : 0;
            // long minId = rafIndex.readLong();
            // long minDate = rafIndex.readLong();
            // long maxId = rafIndex.readLong();
            // long maxDate = rafIndex.readLong();

            List<ElementIndex> sizes = new LinkedList<>();
            long positionInDataFile = DATA_FILE_HEADER_LENGTH;
            while (length > rafIndex.getFilePointer()) {
                // if (lengthData <= positionInDataFile) {
                //     rafIndex.setLength(rafIndex.getFilePointer());
                //     // rafData.setLength(positionInDataFile);
                //     break;
                // }
                ElementIndex elementIndex = readIndexElement(rafIndex, positionInDataFile, countAdditionalBytesIndex, versionIndex);
                sizes.add(elementIndex);
                positionInDataFile += elementIndex.getSize();
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

    synchronized private ElementIndex readIndexElement(RandomAccessFile raf, long position, int countAdditionalBytesInIndex, int ver) throws IOException {
        int size = raf.readInt();
        long id = raf.readLong();
        long date = raf.readLong();
        List<Object> additionalData = null;
        if (countAdditionalBytesInIndex > 0 && funcIndexAdditionalDataConverter != null) {
            byte[] additionalBytes = new byte[countAdditionalBytesInIndex];
            raf.readFully(additionalBytes);
            additionalData = funcIndexAdditionalDataConverter.apply(ver, additionalBytes);
        }
        return new ElementIndex(
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
        int countAdditionalBytesIndex = funcCountAdditionalBytes != null ? funcCountAdditionalBytes.apply(ver) : 0;
        long length = rafLog.length();
        Map<Long, ElementIndex> lastInLog = new HashMap<>();
        while (length > rafLog.getFilePointer()) {
            byte type = rafLog.readByte();
            ElementIndex elementIndex = null;
            if (LOG_ELEMENT_TYPE_SAVE == type) {
                elementIndex = readIndexElement(rafLog, -1, countAdditionalBytesIndex, ver);
                elementIndex.setPositionInLog(elementIndex.getPosition());
                elementIndex.setSizeInLog(elementIndex.getSize());
                if (elementIndex.getSize() > 0) {
                    // byte[] data = new byte[size];
                    // rafLog.readFully(data);
                    /*int countSkipped = */
                    rafLog.skipBytes(elementIndex.getSize());
                }
                lastInLog.put(elementIndex.getId(), elementIndex);
            } else if (LOG_ELEMENT_TYPE_DELETE == type) {
                elementIndex = readIndexElement(rafLog, 0, 0, ver);
                lastInLog.put(elementIndex.getId(), null);
            }
        }
        List<ElementIndex> elementIndices = new LinkedList<>();

        //for update and remove
        for (ElementIndex elementIndex : index.getElements()) {
            if (lastInLog.containsKey(elementIndex.getId())) {
                ElementIndex elementIndexNew = lastInLog.get(elementIndex.getId());
                if (elementIndexNew == null)
                    continue;
                elementIndices.add(new ElementIndex(elementIndex, elementIndexNew));
            } else {
                elementIndices.add(elementIndex);
            }
        }

        //for create
        Map<Long, ElementIndex> idsInIndex = index.getElements().stream().collect(Collectors.toMap(ElementIndex::getId, id -> id));
        lastInLog.values().forEach(i -> {
            if (i == null)
                return;
            if (idsInIndex.containsKey(i.getId()))
                return;
            elementIndices.add(new ElementIndex(null, i));
        });

        return new Index(elementIndices, getVersion());
    }

    synchronized private void applyLog(Index index, boolean reinit) throws IOException {
        if (!isOpen() || rafLog.length() <= LOG_FILE_HEADER_LENGTH)
            return;
        clearTmpFiles();
        try (RandomAccessFile rafIndexNew = new RandomAccessFile(indexFileNew, "rw"); RandomAccessFile rafDataNew = new RandomAccessFile(dataFileNew, "rw")) {
            initNewDB(rafIndexNew, rafDataNew);
            LinkedList<ElementIndex> elementIndices = new LinkedList<>();
            for (ElementIndex elementIndex : index.getElements()) {
                try {
                    RandomAccessFile rafIn = elementIndex.getPositionInLog() != null ? rafLog : rafData;
                    long position = elementIndex.getPositionInLog() != null ? elementIndex.getPositionInLog() : elementIndex.getPosition();
                    byte[] data = new byte[elementIndex.getRealSize() - DATA_FILE_ELEMENT_HEADER_LENGTH];
                    rafIn.seek(position + DATA_FILE_ELEMENT_HEADER_LENGTH);
                    rafIn.readFully(data);
                    List<Object> additionalData = elementIndex.getAdditionalData();
                    if (elementIndex.getVersion() != getVersion() && elementIndex.getPositionInLog() == null && elementIndex.getSizeInLog() == null) {
                        T obj = funcConverter.apply(elementIndex.getVersion(), data);
                        if (obj == null)
                            throw new InvalidObjectException("converter return null");
                        data = funcReverseConverter.apply(getVersion(), obj);
                        if (data == null)
                            throw new InvalidObjectException("rev converter return null");
                        if (funcIndexGetAdditionalData != null)
                            additionalData = funcIndexGetAdditionalData.apply(getVersion(), obj);
                    }

                    writeElementDirect(rafIndexNew, rafDataNew, data.length + DATA_FILE_ELEMENT_HEADER_LENGTH, elementIndex.getId(), elementIndex.getDate(), additionalData, data);
                    elementIndices.add(elementIndex);
                } catch (Exception e) {
                    //if error - not stop operation
                    e.printStackTrace();
                }
            }
            rafIndexNew.setLength(rafIndexNew.getFilePointer());
            rafDataNew.setLength(rafDataNew.getFilePointer());
            index.saveAllNew(elementIndices);
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
                .collect(Collectors.toList()), null);
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
                .map(ElementIndex::getId)
                .collect(Collectors.toList());
    }

    synchronized private Stream<ElementIndex> findIndexElementByIdInRange(long minId, long maxId) {
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
                .collect(Collectors.toList()), null);
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
                .map(ElementIndex::getId)
                .collect(Collectors.toList());
    }

    synchronized private Stream<ElementIndex> findIndexElementByDateInRange(long minDate, long maxDate) {
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
        return fastRead(findIndexElementByPositionInRange(minPosition, maxPosition).collect(Collectors.toList()), null);
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
                .map(ElementIndex::getId)
                .collect(Collectors.toList());
    }

    private Stream<ElementIndex> findIndexElementByPositionInRange(int minPosition, int maxPosition) {
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

        return fastRead(index.getElements().subList(firstId, index.getElements().size()), null);
    }

    synchronized private List<T> fastRead(List<ElementIndex> elementIndexList, byte[] data) {
        if (elementIndexList.isEmpty())
            return new ArrayList<>();
        List<ElementIndex> elementsInData = elementIndexList.stream()
                .filter(e -> e.getPositionInLog() == null && e.getSizeInLog() == null)
                .collect(Collectors.toList());
        long minPosition = 0;
        if (data == null && elementsInData.size() > 5 && elementsInData.size() > (elementIndexList.size() / 2)) {
            minPosition = elementsInData.stream()
                    .min(Comparator.comparingLong(ElementIndex::getPosition))
                    .map(ElementIndex::getPosition)
                    .orElse(elementsInData.get(0).getPosition());
            ElementIndex max = elementsInData.stream()
                    .max(Comparator.comparingLong(ElementIndex::getPosition))
                    .orElse(elementsInData.get(0));
            long maxPosition = max.getPosition();
            int maxSize = max.getSize();
            long size = maxPosition + maxSize - minPosition;
            long sizeElements = elementsInData.stream().mapToLong(ElementIndex::getRealSize).sum();
            if (size > 0 && size < (Integer.MAX_VALUE / 2) && sizeElements * 3 > size) {
                try {
                    data = new byte[(int) size];
                    rafData.seek(minPosition);
                    rafData.readFully(data);
                } catch (Exception e) {
                    data = null;
                }
            }
        }

        List<Long> ids = elementIndexList.stream()
                .map(ElementIndex::getId)
                .collect(Collectors.toList());
        byte[] dataTmp = data;
        long minPositionTmp = minPosition;
        if (data == null || elementIndexList.stream().anyMatch(e -> {
            long position = e.getPosition() - minPositionTmp;
            return !(e.getPositionInLog() == null && e.getSizeInLog() == null && position >= 0 && position < dataTmp.length);
        })) {
            return elementIndexList.stream()
                    .map(e -> {
                        long position = e.getPosition() - minPositionTmp;
                        if (dataTmp != null && e.getPositionInLog() == null && e.getSizeInLog() == null && position >= 0 && position < dataTmp.length) {
                            return readElement(dataTmp, (int) position, e.getSize(), e.getVersion());
                            // for back compatibility
                            // if (funcGetId.apply(element) != e.getId())
                            //     throw new RuntimeException(String.format("wrong data: need id %d but get %s", e.getId(), funcGetId.apply(element)));
                            // return element;
                        } else {
                            return readElement(e);
                        }
                    })
                    .filter(Objects::nonNull)
                    .filter(e -> ids.contains(funcGetId.apply(e)))
                    .collect(Collectors.toList());
        } else {
            return elementIndexList.parallelStream()
                    .map(e -> readElement(dataTmp, (int) (e.getPosition() - minPositionTmp), e.getSize(), e.getVersion()))
                    .filter(Objects::nonNull)
                    .filter(e -> ids.contains(funcGetId.apply(e)))
                    .collect(Collectors.toList());
        }
    }

    /**
     * return all elements
     *
     * @return all elements
     */
    synchronized public List<T> getAll() {
        try {
            /*
            return index.getElements().stream()
                    .map(e -> readElement(data, (int) e.getPosition(), e.getSize()))
                    .collect(Collectors.toList());
            */
            byte[] data = new byte[(int) rafData.length()];
            rafData.seek(0);
            rafData.readFully(data);
            return fastRead(index.getElements(), data);
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
                .map(ElementIndex::getId)
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
    synchronized public List<T> findByFilter(BiPredicate<Integer, List<Object>> filter) {
        return fastRead(findByFilterPrivate(filter).collect(Collectors.toList()), null);
    }

    /**
     * find element ids with use predicate
     * predicate get id (long) and date (long) and all additional data as list
     * fast operation, use index only
     *
     * @param filter Predicate
     * @return list of element ids
     */
    synchronized public List<Long> findIdsByFilter(BiPredicate<Integer, List<Object>> filter) {
        return findByFilterPrivate(filter)
                .map(ElementIndex::getId)
                .collect(Collectors.toList());
    }

    synchronized private Stream<ElementIndex> findByFilterPrivate(BiPredicate<Integer, List<Object>> filter) {
        Objects.requireNonNull(filter);
        return index.getElements().stream()
                .filter(e -> {
                    ArrayList<Object> arrayList = new ArrayList<>((e.getAdditionalData() != null ? e.getAdditionalData().size() : 0) + 2);
                    arrayList.add(e.getId());
                    arrayList.add(e.getDate());
                    if (e.getAdditionalData() != null)
                        arrayList.addAll(e.getAdditionalData());
                    return filter.test(e.getVersion(), arrayList);
                });
    }

    /**
     * find elements with use predicate
     * predicate get IElement
     * fast operation, use index only
     *
     * @param filter Predicate
     * @return list of IElement
     */
    synchronized public List<IElement> findByFilter(Predicate<IElement> filter) {
        Objects.requireNonNull(filter);
        return index.getElements().stream()
                .filter(filter)
                .collect(Collectors.toList());
    }

    /**
     * find elements with use predicate
     * predicate get data element
     * very slow operation
     *
     * @param filter Predicate
     * @return list of elements
     */
    synchronized public List<T> findByDataFilter(BiPredicate<Integer, T> filter) {
        Objects.requireNonNull(filter);
        return index.getElements().stream()
                .map(e -> {
                    T t = readElement(e);
                    return t != null ? Map.entry(e, t) : null;
                })
                .filter(Objects::nonNull)
                .filter(e -> filter.test(e.getKey().getVersion(), e.getValue()))
                .map(Map.Entry::getValue)
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
    synchronized public List<Long> findIdsByDataFilter(BiPredicate<Integer, T> filter) {
        Objects.requireNonNull(filter);
        return index.getElements().stream()
                .map(e -> {
                    T t = readElement(e);
                    return t != null ? Map.entry(e, t) : null;
                })
                .filter(Objects::nonNull)
                .filter(e -> filter.test(e.getKey().getVersion(), e.getValue()))
                .map(Map.Entry::getValue)
                .map(funcGetId)
                .collect(Collectors.toList());
    }

    synchronized private T readElement(ElementIndex e) {
        try {
            RandomAccessFile rafIn = e.getPositionInLog() != null ? rafLog : rafData;
            long position = e.getPositionInLog() != null ? e.getPositionInLog() : e.getPosition();
            rafIn.seek(position + DATA_FILE_ELEMENT_HEADER_LENGTH);
            byte[] data = new byte[e.getRealSize() - DATA_FILE_ELEMENT_HEADER_LENGTH];
            int b = rafIn.read(data);
            if (b != data.length)
                throw new Exception("wrong element size. index damaged.");
            return funcConverter.apply(e.getPositionInLog() != null ? getVersion() : e.getVersion(), data);
            // for back compatibility
            // if (funcGetId.apply(element) != e.getId())
            //     throw new RuntimeException(String.format("wrong data: need id %d but get %s", e.getId(), funcGetId.apply(element)));
            // return element;
        } catch (Exception ex) {
            // throw new RuntimeException("error", ex);
            ex.printStackTrace();
            return null;
        }
    }

    private T readElement(byte[] dataStorage, int position, int size, int version) {
        try {
            if (size <= DATA_FILE_ELEMENT_HEADER_LENGTH || dataStorage.length < position + size)
                throw new IllegalArgumentException("wrong element size. index damaged.");
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

        long minIdOld = index.getMinId();
        long minDateOld = index.getMinDate();
        long maxIdOld = index.getMaxId();
        long maxDateOld = index.getMaxDate();
        List<ElementIndex> elementsOldIndex = new ArrayList<>(index.getElements());
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
            index.getElements().addAll(elementsOldIndex);
            throw e;
        }
    }

    private Index initNewDB(RandomAccessFile rafIndex, RandomAccessFile rafData) throws IOException {
        rafData.seek(0);
        rafData.writeInt(getVersion());
        rafData.setLength(DATA_FILE_HEADER_LENGTH);
        return initNewIndex(rafIndex);
    }

    private Index initNewIndex(RandomAccessFile rafIndex) throws IOException {
        rafIndex.seek(0);
        rafIndex.writeInt(getVersion());
        rafIndex.setLength(INDEX_FILE_HEADER_LENGTH);
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
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos)) {
            long position = rafLog.length();
            rafLog.seek(position);
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
                if (bytes == null)
                    throw new InvalidObjectException("rev converter return null");
                ElementData<T> elementData = new ElementData<>(
                        elementId,
                        elementDate,
                        data);

                int elementSize = DATA_FILE_ELEMENT_HEADER_LENGTH + bytes.length;
                List<Object> additionalData = null;
                if (funcIndexGetAdditionalData != null)
                    additionalData = funcIndexGetAdditionalData.apply(getVersion(), data);
                writeElementToLog(dos, LOG_ELEMENT_TYPE_SAVE, elementSize, elementData.getId(), elementData.getDate(), additionalData, bytes);

                elements.add(elementData.getData());
                position += logFileElementHeaderLength;
                index.saveElement(elementData, additionalData, elementSize, position);
                position += elementSize;
            }
            dos.flush();
            rafLog.write(baos.toByteArray());
            // rafLog.setLength(position);
        }

        return elements;
    }

    synchronized private void operationDelete(int startPosition, int count) throws IOException {
        List<ElementIndex> elementIndices = index.getElements().subList(startPosition, startPosition + count);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos)) {
            long length = rafLog.length();
            rafLog.seek(length);
            for (ElementIndex elementIndex : elementIndices)
                writeElementToLog(dos, LOG_ELEMENT_TYPE_DELETE, 0, elementIndex.getId(), System.currentTimeMillis(), null, null);
            dos.flush();
            byte[] bytes = baos.toByteArray();
            rafLog.write(bytes);
            // rafLog.setLength(length + bytes.length);
        }
        index.removeElements(startPosition, count);
    }

    synchronized private void writeElementDirect(RandomAccessFile rafIndex, RandomAccessFile rafData, int elementSize, long id, long date, List<Object> additionalData, byte[] bytes) throws IOException {
        //check consistent
        if (funcConverter.apply(getVersion(), bytes) == null)
            throw new IllegalArgumentException("wrong data");
        rafData.writeInt(bytes.length);
        rafData.write(bytes);
        writeIndexElement(rafIndex, elementSize, id, date, additionalData);
    }

    synchronized private void writeIndexElement(DataOutput rafIndex, int elementSize, long id, long date, List<Object> additionalData) throws IOException {
        //check consistent
        byte[] additional = null;
        if (additionalData != null && funcIndexAdditionalDataReverseConverter != null)
            additional = funcIndexAdditionalDataReverseConverter.apply(getVersion(), additionalData);
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
        dos.writeByte(type);
        writeIndexElement(dos, elementSize, id, date, additionalData);
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
            ElementIndex elementIndex = index.getElements().get(i);
            if (elementIndex.getId() > id)
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
            ElementIndex elementIndex = index.getElements().get(i);
            if (elementIndex.getId() == id) {
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

    public List<T> findOneByStringIndexKey(String key, int indexId) {
        Objects.requireNonNull(key);
        int id = 2 + indexId;
        return findByFilter((ver, list) -> list.size() > id && Objects.equals(list.get(id).toString().stripTrailing(), key));
    }

    public List<T> findOneByStringIndexKey(String key, int indexId, int maxFieldSize, Function<T, String> getter) {
        if (key.length() <= maxFieldSize) {
            return findOneByStringIndexKey(key, indexId);
        } else {
            Objects.requireNonNull(key);
            Objects.requireNonNull(getter);
            int id = 2 + indexId;
            return findByFilter((ver, list) -> list.size() > id && key.startsWith(list.get(id).toString().stripTrailing())).stream()
                    .filter(o -> Objects.equals(getter.apply(o), key))
                    .collect(Collectors.toList());
        }
    }

    /**
     * get all elements
     * return data only from index in memory
     * fast operation
     *
     * @return List<IElement>
     */
    public List<IElement> getIndexElements() {
        return new ArrayList<>(index.getElements());
    }

    /**
     * rebuild index and save in file
     * use only if additional data changed
     *
     * @throws IOException
     */
    synchronized public void rebuildIndex() throws IOException {
        close();
        open();
        if (funcIndexGetAdditionalData == null || funcCountAdditionalBytes == null)
            return;
        this.logFileElementHeaderLength = LOG_FILE_ELEMENT_HEADER_LENGTH + funcCountAdditionalBytes.apply(version);
        if (indexFileNew.exists())
            indexFileNew.delete();
        try {
            try (RandomAccessFile rafIndexTmp = new RandomAccessFile(indexFileNew, "rw")) {
                initNewIndex(rafIndexTmp);
                for (ElementIndex e : index.getElements()) {
                    T element = readElement(e);
                    List<Object> additionalData = funcIndexGetAdditionalData.apply(version, element);
                    e.setAdditionalData(additionalData);
                    writeIndexElement(rafIndexTmp, e.getRealSize(), e.getId(), e.getDate(), e.getAdditionalData());
                }
            }
            if (rafIndex != null) {
                rafIndex.close();
                rafIndex = null;
            }
            indexFile.delete();
            indexFileNew.renameTo(indexFile);
            rafIndex = new RandomAccessFile(indexFile, "rw");
        } catch (IOException e) {
            indexFileNew.delete();
            if (rafIndex == null && indexFile.exists())
                rafIndex = new RandomAccessFile(indexFile, "rw");
            throw e;
        }
    }

}
