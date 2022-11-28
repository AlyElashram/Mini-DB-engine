import com.opencsv.CSVWriter;
import java.io.*;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface {
    boolean caught = false;
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void init() {

        try {
            File file = new File("src\\main\\resources\\data");
            if (!file.exists()) {
                file.mkdir();
                System.out.println("File Created");
            }
        } catch (Exception e) {
        }
    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        // Write the MetaData for this table
        try {
            CSVWriter writer = new CSVWriter(new FileWriter("src\\main\\resources\\" + tableName + ".csv"));
            Vector<String> toWrite = new Vector<String>();
            Vector<String> names = new Vector<String>();
            Vector<String> types = new Vector<String>();
            Vector<String> min = new Vector<String>();
            Vector<String> max = new Vector<String>();
            colNameType.forEach((k, v) -> {
                names.add(k);
                types.add(v);
            });
            colNameMin.forEach((k, v) -> {
                min.add(v);
            });
            colNameMax.forEach((k, v) -> {
                max.add(v);
            });

            for (int i = 0; i < names.size(); i++) {
                toWrite.add(tableName);
                toWrite.add(names.elementAt(i));
                toWrite.add(types.elementAt(i));
                if (names.elementAt(i).equals(clusteringKey)) {
                    toWrite.add("True");
                } else {
                    toWrite.add("False");
                }
                toWrite.add("False");
                toWrite.add(min.elementAt(i));
                toWrite.add(max.elementAt(i));
                String[] x = new String[toWrite.size()];
                for (int j = 0; j < toWrite.size(); j++) {
                    x[j] = toWrite.elementAt(j);

                }
                writer.writeNext(x);
                toWrite.clear();

            }
            writer.flush();

        } catch (IOException e) {
        }
        // Create the Table and serialize it
        try {
            Table name = new Table(tableName);
            FileOutputStream file1 = new FileOutputStream("src\\main\\resources\\data\\" + tableName + "_table.class");
            ObjectOutputStream out = new ObjectOutputStream(file1);
            // Method for serialization of object
            out.writeObject(name);
            out.close();
            file1.close();
        } catch (Exception e) {
        }

    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        Table myTable = deserializeTable(tableName);
        Grid d = new Grid(columnNames, tableName);
        for (int k = 0; k < columnNames.length; k++) {
            for (int i = 1; i <= myTable.numberOfPages; i++) {
                Pages page = deserializePage(tableName, i);
                for (int j = 0; j < page.allRecords.size(); j++) {
                    Record a = page.allRecords.elementAt(j);
                    int index = getRange(d, a, columnNames[k]);
                    Bucket bucket = d.gridIndex[index];
                    if (bucket == null) {
                        // create new bucket we insert feeh 3ala tool
                        Bucket ayhaga = new Bucket(tableName, myTable.bucketSize);
                        ayhaga.addRef(new References(i, j));
                    } else {
                        // insert
                        bucket.addRef(new References(i, j));
                    }
                }
            }
        }
        myTable.addGrid(d);
        serializeTable(tableName, myTable);
    }

    public static int getRange(Grid d, Record a, String columnName) {
        String index = "";
        for (int i = 0; i < a.tuples.size(); i++) {
            // da beyerga3ly empty bas msh null
            // Vector<Range> range =
            // d.colNameRange.get(a.tuples.get(i).getColName().toString());
            Vector<Range> range = d.colNameRange.get(columnName);
            if (range != null) {
                if (!range.isEmpty()) {
                    Tuple tuple = a.tuples.get(i);
                    String type = tuple.type;
                    Object value = tuple.getValue();
                    for (int j = 0; j < range.size(); j++) {
                        Object lowerB = range.elementAt(j).lowerB;
                        Object upperB = range.elementAt(j).upperB;
                        if (compare(type, value, lowerB) >= 0 && compare(type, value, upperB) <= 0) {
                            index += j;
                        }
                    }
                }
            }
        }
        return Integer.parseInt(index);

    }

    public static int compare(String keyType, Object a, Object b) {
        if (keyType.equals('"' + "java.lang.Integer" + '"')) {
            int minCheck1 = Integer.parseInt(a.toString());
            int minimum1 = Integer.parseInt(b.toString());
            return minCheck1 - minimum1;
        } else if (keyType.equals('"' + "java.lang.Double" + '"')) {
            BigDecimal minCheck1 = new BigDecimal(a.toString());
            BigDecimal minimum1 = new BigDecimal(b.toString());
            return minCheck1.compareTo(minimum1);
        }

        else if (keyType.equals('"' + "java.lang.String" + '"')) {
            String minCheck1 = a.toString();
            String minimum1 = b.toString();
            return minCheck1.compareTo(minimum1);
        } else {
            Date minCheck1 = (Date) a;
            Date minimum1 = (Date) b;
            return minCheck1.compareTo(minimum1);
        }
    }

    public static void Sort(String keyType, Vector<Record> a) throws Exception {
        if (keyType.equals("class java.lang.Integer")) {
            mergeSortInt(a, 0, a.size() - 1);

        } else if (keyType.equals("class java.math.BigDecimal")) {
            mergeSortDouble(a, 0, a.size() - 1);
        } else if (keyType.equals("class java.lang.String")) {
            mergeSort(a, 0, a.size() - 1);

        } else {
            mergeSortDate(a, 0, a.size() - 1);

        }
    }

    public static void mergeSort(Vector<Record> a, int from, int to) {
        if (from == to) {
            return;
        }
        int mid = (from + to) / 2;
        // sort the first and the second half
        mergeSort(a, from, mid);
        mergeSort(a, mid + 1, to);
        merge(a, from, mid, to);
    }// end mergeSort

    public static void merge(Vector<Record> a, int from, int mid, int to) {
        int n = to - from + 1; // size of the range to be merged
        Vector<Record> b = new Vector<>(); // merge both halves into a temporary array b
        for (int i = 0; i < n; i++) {
            b.add(null);
        }
        int i1 = from; // next element to consider in the first range
        int i2 = mid + 1; // next element to consider in the second range
        int j = 0; // next open position in b

        // as long as neither i1 nor i2 past the end, move the smaller into b
        while (i1 <= mid && i2 <= to) {
            if (a.elementAt(i1).tuples.elementAt(0).getValue().toString().toLowerCase()
                    .compareTo(a.elementAt(i2).tuples.elementAt(0).getValue().toString().toLowerCase()) < 0) {
                b.setElementAt(a.elementAt(i1), j);
                i1++;
            } else {
                b.setElementAt(a.elementAt(i2), j);
                i2++;
            }
            j++;
        }

        // note that only one of the two while loops below is executed
        // copy any remaining entries of the first half
        while (i1 <= mid) {
            b.setElementAt(a.elementAt(i1), j);
            i1++;
            j++;
        }

        // copy any remaining entries of the second half
        while (i2 <= to) {
            b.setElementAt(a.elementAt(i2), j);
            i2++;
            j++;
        }

        // copy back from the temporary array
        for (j = 0; j < n; j++) {
            a.setElementAt(b.elementAt(j), from + j);
        }
    }

    public static void mergeSortInt(Vector<Record> a, int from, int to) throws DBAppException {
        if (from == to) {
            return;
        }
        int mid = (from + to) / 2;
        // sort the first and the second half
        mergeSortInt(a, from, mid);
        mergeSortInt(a, mid + 1, to);
        mergeInt(a, from, mid, to);
    }// end mergeSort

    public static void mergeInt(Vector<Record> a, int from, int mid, int to) throws DBAppException {
        try {
            int n = to - from + 1; // size of the range to be merged
            Vector<Record> b = new Vector<>(); // merge both halves into a temporary array b
            for (int i = 0; i < n; i++) {
                b.add(null);
            }
            int i1 = from; // next element to consider in the first range
            int i2 = mid + 1; // next element to consider in the second range
            int j = 0; // next open position in b

            // as long as neither i1 nor i2 past the end, move the smaller into b
            while (i1 <= mid && i2 <= to) {

                if (Integer.parseInt(a.elementAt(i1).key) < Integer.parseInt(a.elementAt(i2).key)) {
                    b.setElementAt(a.elementAt(i1), j);
                    i1++;
                } else {
                    b.setElementAt(a.elementAt(i2), j);
                    i2++;
                }
                j++;
            }

            // note that only one of the two while loops below is executed
            // copy any remaining entries of the first half
            while (i1 <= mid) {
                b.setElementAt(a.elementAt(i1), j);
                i1++;
                j++;
            }

            // copy any remaining entries of the second half
            while (i2 <= to) {
                b.setElementAt(a.elementAt(i2), j);
                i2++;
                j++;
            }

            // copy back from the temporary array
            for (j = 0; j < n; j++) {
                a.setElementAt(b.elementAt(j), from + j);
            }
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public static void mergeSortDouble(Vector<Record> a, int from, int to) {
        if (from == to) {
            return;
        }
        int mid = (from + to) / 2;
        // sort the first and the second half
        mergeSortDouble(a, from, mid);
        mergeSortDouble(a, mid + 1, to);
        mergeDouble(a, from, mid, to);
    }// end mergeSort

    public static void mergeDouble(Vector<Record> a, int from, int mid, int to) {
        int n = to - from + 1; // size of the range to be merged
        Vector<Record> b = new Vector<>(); // merge both halves into a temporary array b
        for (int i = 0; i < n; i++) {
            b.add(null);
        }
        int i1 = from; // next element to consider in the first range
        int i2 = mid + 1; // next element to consider in the second range
        int j = 0; // next open position in b

        // as long as neither i1 nor i2 past the end, move the smaller into b
        while (i1 <= mid && i2 <= to) {
            BigDecimal first = new BigDecimal(a.elementAt(i1).key);
            BigDecimal second = new BigDecimal(a.elementAt(i2).key);
            if (first.compareTo(second) < 0) {
                b.setElementAt(a.elementAt(i1), j);
                i1++;
            } else {
                b.setElementAt(a.elementAt(i2), j);
                i2++;
            }
            j++;
        }

        // note that only one of the two while loops below is executed
        // copy any remaining entries of the first half
        while (i1 <= mid) {
            b.setElementAt(a.elementAt(i1), j);
            i1++;
            j++;
        }

        // copy any remaining entries of the second half
        while (i2 <= to) {
            b.setElementAt(a.elementAt(i2), j);
            i2++;
            j++;
        }

        // copy back from the temporary array
        for (j = 0; j < n; j++) {
            a.setElementAt(b.elementAt(j), from + j);
        }
    }

    public static void mergeSortDate(Vector<Record> a, int from, int to) throws ParseException {
        if (from == to) {
            return;
        }
        int mid = (from + to) / 2;
        // sort the first and the second half
        mergeSortDate(a, from, mid);
        mergeSortDate(a, mid + 1, to);
        mergeDate(a, from, mid, to);
    }// end mergeSort

    public static void mergeDate(Vector<Record> a, int from, int mid, int to) throws ParseException {
        int n = to - from + 1; // size of the range to be merged
        Vector<Record> b = new Vector<>(); // merge both halves into a temporary array b
        for (int i = 0; i < n; i++) {
            b.add(null);
        }
        int i1 = from; // next element to consider in the first range
        int i2 = mid + 1; // next element to consider in the second range
        int j = 0; // next open position in b

        // as long as neither i1 nor i2 past the end, move the smaller into b
        while (i1 <= mid && i2 <= to) {
            Date key1 = (Date) a.elementAt(i1).Datekey;
            Date key2 = (Date) a.elementAt(i2).Datekey;
            if (key1.compareTo(key2) < 0) {
                b.setElementAt(a.elementAt(i1), j);
                i1++;
            } else {
                b.setElementAt(a.elementAt(i2), j);
                i2++;
            }
            j++;
        }

        // note that only one of the two while loops below is executed
        // copy any remaining entries of the first half
        while (i1 <= mid) {
            b.setElementAt(a.elementAt(i1), j);
            i1++;
            j++;
        }

        // copy any remaining entries of the second half
        while (i2 <= to) {
            b.setElementAt(a.elementAt(i2), j);
            i2++;
            j++;
        }

        // copy back from the temporary array
        for (j = 0; j < n; j++) {
            a.setElementAt(b.elementAt(j), from + j);
        }
    }

    public int search(String keyType, Vector<Record> arr, Object x) {
        int index;
        if (keyType.equals("class java.lang.Integer")) {
            index = binarySearchInt(arr, 0, arr.size() - 1, Integer.parseInt(x.toString()));
        } else if (keyType.equals("class java.math.BigDecimal")) {
            BigDecimal x1 = new BigDecimal(x.toString());
            index = binarySearchDouble(arr, 0, arr.size() - 1, x1);

        } else if (keyType.equals("class java.lang.String")) {
            index = binarySearch(arr, 0, arr.size() - 1, x.toString());
        } else {
            SimpleDateFormat format = new SimpleDateFormat("MM-dd-yyyy");
            Date x1;
            try {
                if (x.toString().length() > 10) {
                    x1 = (Date) x;
                } else {
                    x1 = format.parse(x.toString());
                }
                index = binarySearchDate(arr, 0, arr.size() - 1, x1);
            } catch (ParseException e) {
                index = -1;
            }

        }
        return index;
    }

    public int binarySearch(Vector<Record> arr, int l, int r, String x) {
        if (r >= l) {
            int mid = l + (r - l) / 2;

            // If the element is present at the
            // middle itself
            if (arr.elementAt(mid).key.equals(x.toString()))
                return mid;

            // If element is smaller than mid, then
            // it can only be present in left subarray
            if (arr.elementAt(mid).key.compareTo(x) > 0)
                return binarySearch(arr, l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            return binarySearch(arr, mid + 1, r, x);
        }

        // We reach here when element is not present
        // in array
        return -1;
    }

    public int binarySearchInt(Vector<Record> arr, int l, int r, int x) {
        if (r >= l) {
            int mid = l + (r - l) / 2;

            // If the element is present at the
            // middle itself
            if (arr.elementAt(mid).key.equals(x + ""))
                return mid;

            // If element is smaller than mid, then
            // it can only be present in left subarray
            // arr.elementAt(mid).key.compareTo(x) > 0
            if (Integer.parseInt(arr.elementAt(mid).key) > x)
                return binarySearchInt(arr, l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            return binarySearchInt(arr, mid + 1, r, x);
        }

        // We reach here when element is not present
        // in array
        return -1;
    }

    public int binarySearchDouble(Vector<Record> arr, int l, int r, BigDecimal x) {
        if (r >= l) {
            int mid = l + (r - l) / 2;

            // If the element is present at the
            // middle itself

            if (arr.elementAt(mid).key.equals(x.toString())) {
                return mid;
            }

            // If element is smaller than mid, then
            // it can only be present in left subarray
            // arr.elementAt(mid).key.compareTo(x) > 0
            BigDecimal ky = new BigDecimal(arr.elementAt(mid).key);
            if (ky.compareTo(x) > 0)
                return binarySearchDouble(arr, l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            return binarySearchDouble(arr, mid + 1, r, x);
        }

        // We reach here when element is not present
        // in array
        return -1;
    }

    public int binarySearchDate(Vector<Record> arr, int l, int r, Date x) {
        if (r >= l) {
            int mid = l + (r - l) / 2;

            // If the element is present at the
            // middle itself
            if (arr.elementAt(mid).Datekey.equals(x))
                return mid;

            // If element is smaller than mid, then
            // it can only be present in left subarray
            if (arr.elementAt(mid).Datekey.compareTo(x) > 0)
                return binarySearchDate(arr, l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            return binarySearchDate(arr, mid + 1, r, x);
        }

        // We reach here when element is not present
        // in array
        return -1;
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        Vector<Tuple> allTuples = new Vector<>();
        colNameValue.forEach((k, v) -> {
            try {
                allTuples.add(new Tuple(k, v, tableName));
            } catch (Exception e) {
                caught = true;
                return;
            }
        });
        if (caught) {
            caught = false;
            throw new DBAppException("Hashtable entered is incorrect");
        }
        Record toBeAdded = new Record(tableName, allTuples);
        String trialKey = toBeAdded.key;
        Date dateKey = toBeAdded.Datekey;
        String keyType = toBeAdded.keyType;
        if (trialKey == null && dateKey == null) {
            throw new DBAppException("Key is null");
        }
        Object key;
        if (toBeAdded.key == null) {
            key = toBeAdded.Datekey;
        } else {
            key = toBeAdded.key;
        }

        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream("src\\main\\resources\\data\\" + tableName + "_table.class");
            ObjectInputStream in = new ObjectInputStream(file);
            // Read the table from memory
            Table table = (Table) in.readObject();
            int numberOfPages = table.numberOfPages;
            in.close();
            file.close();

            if (table.index != null) {
                // insertIntoIndex()

            }

            if (numberOfPages == 0) {
                Pages page = new Pages(toBeAdded);
                table.addRecord(1);
                table.addPage();
                table.setKeyType(toBeAdded.keyType, page.pageSize);
                table.setMin(1, toBeAdded);
                table.setMax(1, toBeAdded);
                serializePage(tableName, page, 1);
                serializeTable(tableName, table);
            } else if (numberOfPages == 1) {
                Object minOfPage = table.minOfPage.get(1);
                Object maxOfPage = table.maxOfPage.get(1);
                // check law ana a2al men el minimum law ah deserialize page we hoteny ana fel
                // awal
                // then ha shift el tany
                // else if law ana in range ha insert we a sort we a shift akher record
                // else hoteny ana fe page gedeeda

                // law el page feha makan insert 3ala tool
                Pages page1 = deserializePage(tableName, 1);
                if (page1.allRecords.size() != page1.pageSize) {
                    page1.allRecords.add(toBeAdded);
                    table.addRecord(1);
                    table.setMax(1, toBeAdded);
                    table.setMin(1, toBeAdded);
                    Sort(keyType, page1.allRecords);
                    serializeTable(tableName, table);
                    serializePage(tableName, page1, 1);
                }

                // check law ana a2al mel minimum we zabat el denya
                else if (compareTo(keyType, key, minOfPage) <= 0) {
                    Pages page = deserializePage(tableName, 1);
                    Record toBeShited = page.allRecords.get(page.allRecords.size() - 1);
                    page.allRecords.insertElementAt(toBeAdded, 0);
                    page.allRecords.removeElementAt(page.allRecords.size() - 1);
                    Pages extraPage = new Pages(toBeShited);
                    table.addPage();
                    table.setMin(1, toBeAdded);
                    table.setMax(1, toBeAdded);
                    table.setMin(2, toBeShited);
                    table.setMax(2, toBeShited);
                    table.addRecord(2);
                    serializePage(tableName, page, 1);
                    serializePage(tableName, extraPage, 2);
                    serializeTable(tableName, table);
                }
                // check law ana in range we zabat el denya
                else if (compareTo(keyType, key, minOfPage) >= 0 && compareTo(keyType, key, maxOfPage) < 0) {
                    Pages page = deserializePage(tableName, 1);
                    Record toBeShifted = page.allRecords.elementAt(page.allRecords.size() - 1);
                    page.allRecords.removeElementAt(page.allRecords.size() - 1);
                    page.allRecords.add(toBeAdded);
                    Sort(keyType, page.allRecords);
                    Pages extraPage = new Pages(toBeShifted);
                    table.addPage();
                    table.setMin(1, toBeAdded);
                    // Remove old max since it is shifted
                    // Find new max since max will be shifted
                    table.removeMax(1);
                    table.setMax(1, page.allRecords.elementAt(page.allRecords.size() - 2));

                    table.setMax(1, toBeAdded);
                    table.setMin(2, toBeShifted);
                    table.setMax(2, toBeShifted);
                    table.addRecord(2);
                    serializePage(tableName, page, 1);
                    serializePage(tableName, extraPage, 2);
                    serializeTable(tableName, table);

                } else {
                    Pages extraPage = new Pages(toBeAdded);
                    table.addPage();
                    table.setMin(2, toBeAdded);
                    table.setMax(2, toBeAdded);
                    table.addRecord(2);
                    serializePage(tableName, extraPage, 2);
                    serializeTable(tableName, table);
                }
            }

            else {
                int pageIndex = -1;
                // 3andy hena 2 cases bas
                // awel case en ana msh in range of wala wahda sa3etha a3mel page gedeeda we
                // khalas
                // ana fe range wahda menhom we de split into two cases
                // case en de akher wahda sa3etha a3mel page gedeeda
                // case en de msh akher page sa3etha ha3mel mawal el overflow

                // Loop to check law fe ay page a2dar a insert feha we law feeh store el index
                // dah
                for (int i = 1; i <= numberOfPages; i++) {
                    Object minOfPage = table.minOfPage.get(i);
                    Object maxOfPage = table.maxOfPage.get(i);
                    int numberOfRecords = table.numberOfRecords.get(i);
                    if (numberOfRecords != table.pageSize) {
                        pageIndex = i;
                        break;
                    }
                    if (compareTo(keyType, key, minOfPage) < 0) {
                        pageIndex = i;
                        break;
                    }
                    if (compareTo(keyType, key, minOfPage) >= 0 && compareTo(keyType, key, maxOfPage) < 0) {
                        pageIndex = i;
                        break;
                    }
                }
                // mala2etsh wala page a2dar a insert feha aw el page de el akheera we mafehash
                // makan
                // yeb2a ha create page gedeeda we insert feeha
                // we hazabat el minimum wel max beta3ha we hazabat el table we a serialize
                // table wel page el gedeeda

                // mala2etsh page a insert feha ha create
                if (pageIndex == -1) {
                    Pages page = new Pages(toBeAdded);
                    table.addPage();
                    table.setMin(table.numberOfPages, toBeAdded);
                    table.setMax(table.numberOfPages, toBeAdded);
                    table.addRecord(table.numberOfPages);
                    serializeTable(tableName, table);
                    serializePage(tableName, page, table.numberOfPages);
                }

                // la2et page bas heya akher page ha check law heya full ha create law la2
                // insert 3ady
                else if (pageIndex == table.numberOfPages) {
                    Pages akherpage = deserializePage(tableName, pageIndex);
                    // page full
                    if (akherpage.allRecords.size() == akherpage.pageSize) {
                        Pages page = new Pages(toBeAdded);
                        table.addPage();
                        table.addRecord(table.numberOfPages);
                        table.setMin(table.numberOfPages, toBeAdded);
                        table.setMax(table.numberOfPages, toBeAdded);
                        serializeTable(tableName, table);
                        serializePage(tableName, page, table.numberOfPages);
                    }
                    // feeha makan 3ady ha insert we sort
                    else {
                        akherpage.allRecords.add(toBeAdded);
                        table.addRecord(table.numberOfPages);
                        Sort(keyType, akherpage.allRecords);
                        table.setMin(table.numberOfPages, toBeAdded);
                        table.setMax(table.numberOfPages, toBeAdded);
                        serializePage(tableName, akherpage, table.numberOfPages);
                        serializeTable(tableName, table);
                    }

                }

                // la2et page ana in range feha fa ha check for overflow we azabat el denya
                else {
                    // de el page el ana mehtag a insert feeha
                    Pages inRange = deserializePage(tableName, pageIndex);
                    // check law el page full heya lazem tekoon full 3ashan law msh full
                    // kanet hateb2a akher page we kont hakhosh fel ifaya el ablaha
                    if (inRange.allRecords.size() == inRange.pageSize) {
                        // heya full fa ha check el ba3daha law full
                        Pages inRangePlus1 = deserializePage(tableName, pageIndex + 1);
                        // check law heya full bardo
                        if (inRangePlus1.allRecords.size() == inRangePlus1.pageSize) {
                            // el page el ba3daha bardo full
                            // hacheck law leha over Flow a insert feha
                            try {
                                Pages overflow = deserializeOverflow(tableName, pageIndex);
                                overflow.allRecords.add(toBeAdded);
                                serializeOverflow(tableName, overflow, pageIndex);
                                serializeTable(tableName, table);
                            } catch (Exception e) {
                                // malhash overflow fa ha create ana el overflow
                                Pages overflow = new Pages(toBeAdded);
                                serializeOverflow(tableName, overflow, pageIndex);
                                serializeTable(tableName, table);
                            }

                        }

                        // tab law el ba3daha msh full
                        // hakhod akher record a7oto fel page el tahty we asheelo men 3andy we a sort el
                        // etein we a serialize
                        else {
                            Record toBeShifted = inRange.allRecords.get(inRange.allRecords.size() - 1);
                            inRange.allRecords.removeElementAt(inRange.allRecords.size() - 1);
                            inRange.allRecords.add(toBeAdded);
                            inRangePlus1.allRecords.add(toBeShifted);
                            Sort(keyType, inRange.allRecords);
                            Sort(keyType, inRangePlus1.allRecords);
                            table.setMin(pageIndex, toBeAdded);
                            table.setMax(pageIndex, toBeAdded);
                            table.removeMax(pageIndex);
                            table.setMax(pageIndex, inRange.allRecords.elementAt(inRange.allRecords.size() - 2));
                            table.setMin(pageIndex + 1, toBeShifted);
                            table.setMax(pageIndex + 1, toBeShifted);
                            table.addRecord(pageIndex + 1);
                            serializePage(tableName, inRange, pageIndex);
                            serializePage(tableName, inRangePlus1, pageIndex + 1);
                            serializeTable(tableName, table);
                        }

                    }
                }

            }

        } catch (Exception e) {
            // Table was not found

            throw new DBAppException(e.getMessage().toString());
        }
    }

    public static int compareTo(String keyType, Object a, Object b) {
        if (keyType.equals("class java.lang.Integer")) {
            int minCheck1 = Integer.parseInt(a.toString());
            int minimum1 = Integer.parseInt(b.toString());
            return minCheck1 - minimum1;
        } else if (keyType.equals("class java.math.BigDecimal")) {
            BigDecimal minCheck1 = new BigDecimal(a.toString());
            BigDecimal minimum1 = new BigDecimal(b.toString());
            return minCheck1.compareTo(minimum1);
        }

        else if (keyType.equals("class java.lang.String")) {
            String minCheck1 = a.toString();
            String minimum1 = b.toString();
            return minCheck1.compareTo(minimum1);
        } else {
            Date minCheck1 = (Date) a;
            Date minimum1 = (Date) b;
            return minCheck1.compareTo(minimum1);
        }
    }

    public static void serializePage(String tableName, Object page, int pageNumber) {
        // Serialization

        try {
            // Saving of object in a file
            FileOutputStream file = new FileOutputStream(
                    "src\\main\\resources\\data\\" + tableName + pageNumber + ".class");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(page);

            out.close();
            file.close();

        }

        catch (IOException ex) {
            System.out.println("IOException is caught");
        }
    }

    public static void serializeTable(String tableName, Object table) {
        // Serialization
        try {
            // Saving of object in a file
            FileOutputStream file = new FileOutputStream("src\\main\\resources\\data\\" + tableName + "_table.class");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(table);

            out.close();
            file.close();

            System.out.println("Object has been serialized in : " + tableName);

        }

        catch (IOException ex) {
            System.out.println("IOException is caught");
        }
    }

    public static Pages deserializePage(String tableName, int pageNumber) {
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(

                    "src\\main\\resources\\data\\" + tableName + pageNumber + ".class");
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            Pages toParse = (Pages) in.readObject();
            in.close();
            file.close();
            return toParse;
        } catch (Exception e) {
            return null;
        }
    }

    public static Pages deserializeOverflow(String tableName, int pageNumber) {
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(
                    "src\\main\\resources\\data\\" + tableName + pageNumber + "overflow" + ".class");
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            Pages toParse = (Pages) in.readObject();
            in.close();
            file.close();
            return toParse;
        } catch (Exception e) {
            return null;
        }
    }

    public static void serializeOverflow(String tableName, Object page, int pageNumber) {
        // Serialization

        try {
            // Saving of object in a file
            FileOutputStream file = new FileOutputStream(
                    "src\\main\\resources\\data\\" + tableName + pageNumber + "overflow.class");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(page);

            out.close();
            file.close();

            System.out.println("Object has been serialized in : " + "data" + tableName + pageNumber + "overflow.class");

        }

        catch (IOException ex) {
            System.out.println("IOException is caught");
        }
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
            throws DBAppException {
        // Hashtable to tuples
        Vector<Tuple> allTuples = new Vector<>();
        columnNameValue.forEach((k, v) -> {
            try {
                allTuples.add(new Tuple(k, v, tableName));
            } catch (Exception e) {
                caught = true;
                return;
            }
        });
        if (caught) {
            caught = false;
            throw new DBAppException("Hashtable values are incorrect");
        }

        Table table = deserializeTable(tableName);
        int pageIndex = -1;
        for (int i = 1; i <= table.numberOfPages; i++) {
            Object minOfPage = table.minOfPage.get(i);
            Object maxOfPage = table.maxOfPage.get(i);
            if (table.keyType.equals("class java.util.Date")) {
                DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                Date cluster;
                try {
                    cluster = format.parse(clusteringKeyValue);
                    if (compareTo(table.keyType, cluster, minOfPage) >= 0
                            && compareTo(table.keyType, cluster, maxOfPage) <= 0) {
                        pageIndex = i;
                        break;
                    }
                } catch (ParseException e) {
                }

            } else if (compareTo(table.keyType, clusteringKeyValue, minOfPage) >= 0
                    && compareTo(table.keyType, clusteringKeyValue, maxOfPage) <= 0) {
                pageIndex = i;
                break;
            }
        }
        if (pageIndex == -1) {
            throw new DBAppException("Record Not Found");
        } else {
            Pages page = deserializePage(tableName, pageIndex);
            int index = search(table.keyType, page.allRecords, clusteringKeyValue);
            if (index == -1) {
                try {
                    Pages overflow = deserializeOverflow(tableName, pageIndex);
                    index = search(table.keyType, overflow.allRecords, clusteringKeyValue);
                    if (index == -1) {
                        throw new DBAppException("Record not found");
                    }
                    // record mawgood fel over flow
                    else {
                        Record updatable = overflow.allRecords.elementAt(index);
                        while (!allTuples.isEmpty()) {
                            for (int i = 0; i < updatable.tuples.size(); i++) {
                                if (allTuples.elementAt(0).equals(updatable.tuples.elementAt(i))) {
                                    updatable.tuples.setElementAt(allTuples.elementAt(0), i);
                                    allTuples.removeElementAt(0);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new DBAppException("Record not found");
                }
            }

            else {
                Record updatable = page.allRecords.elementAt(index);
                int remove = 0;
                while (!allTuples.isEmpty()) {
                    for (int i = 0; i < updatable.tuples.size(); i++) {
                        if (allTuples.elementAt(0).getValue().toString()
                                .equals(updatable.tuples.elementAt(i).getValue().toString())) {
                            updatable.tuples.setElementAt(allTuples.elementAt(0), i);
                            allTuples.removeElementAt(0);

                        }
                        remove++;
                        if (remove == 10) {
                            return;
                        }
                    }

                }
            }

        }

    }

    public static Table deserializeTable(String tableName) throws DBAppException {
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream("src\\main\\resources\\data\\" + tableName + "_table.class");
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            Table toParse = (Table) in.readObject();
            in.close();
            file.close();
            return toParse;
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        // Hashtable to tuples
        Vector<Tuple> allTuples = new Vector<>();
        Table myTable = deserializeTable(tableName);
        boolean primaryKeyGiven = false;
        Tuple primaryKey = null;
        columnNameValue.forEach((k, v) -> {
            try {
                allTuples.add(new Tuple(k, v, tableName));
            } catch (Exception e) {
                caught = true;
                return;
            }
        });
        if (caught) {
            caught = false;
            throw new DBAppException("Hashtable values are incorrect");
        }
        for (int i = 0; i < allTuples.size(); i++) {
            if (allTuples.elementAt(i).isClustering()) {
                primaryKeyGiven = true;
                primaryKey = allTuples.elementAt(i);
            }
        }
        if (primaryKeyGiven && primaryKey != null) {
            int pageIndex = -1;
            for (int i = 1; i <= myTable.numberOfPages; i++) {
                Object minOfPage = myTable.minOfPage.get(i);
                Object maxOfPage = myTable.maxOfPage.get(i);
                if (compareTo(myTable.keyType, primaryKey.getValue(), minOfPage) >= 0
                        && compareTo(myTable.keyType, primaryKey.getValue(), maxOfPage) <= 0) {
                    pageIndex = i;
                    break;
                }
            }
            if (pageIndex == -1) {
                throw new DBAppException("Record was not found");
            } else {
                // binary search on record
                // law msh la2eeh dawar fe overflow law msh la2eeh bardo throw new
                // dbappexception
                Pages page = deserializePage(tableName, pageIndex);
                int recordIndex = search(myTable.keyType, page.allRecords, primaryKey.getValue());
                if (recordIndex == -1) {
                    try {
                        Pages overflow = deserializeOverflow(tableName, pageIndex);
                        recordIndex = search(myTable.keyType, overflow.allRecords, primaryKey.getValue());
                        if (recordIndex == -1) {
                            throw new DBAppException("Record not found");
                        } else {
                            overflow.allRecords.removeElementAt(recordIndex);
                        }

                    } catch (Exception e) {
                        throw new DBAppException("record not found");
                    }

                } else {
                    // la2et el record el mafrood asheelo we a shift kol el pages el tahty fo2 by
                    // one record
                    // we a zbt el minimum wel maximum beta3 kol page
                    page.allRecords.removeElementAt(recordIndex);
                    myTable.removeRecord(pageIndex);
                    // check if it has overflow then khod men el overflow we hot feeha we sort we
                    // zabat min we max
                    try {
                        Pages overflow = deserializeOverflow(tableName, pageIndex);
                        Record toBeAddedd = overflow.allRecords.elementAt(0);
                        overflow.allRecords.remove(0);
                        page.allRecords.add(toBeAddedd);
                        Sort(myTable.keyType, page.allRecords);
                        myTable.removeMin(pageIndex);
                        myTable.setMin(pageIndex, page.allRecords.elementAt(0));
                        myTable.removeMax(pageIndex);
                        myTable.setMax(pageIndex, page.allRecords.elementAt(page.allRecords.size() - 1));
                        serializePage(tableName, page, pageIndex);
                        serializeOverflow(tableName, overflow, pageIndex);
                    } catch (Exception e) {
                        // malhash overflow
                        try {
                            for (int i = pageIndex; i < myTable.numberOfPages; i++) {
                                if (i == pageIndex) {
                                    Pages pageAfter = deserializePage(tableName, i + 1);
                                    Record toBeShited1 = pageAfter.allRecords.elementAt(0);
                                    pageAfter.allRecords.removeElementAt(0);
                                    page.allRecords.add(toBeShited1);
                                    Sort(myTable.keyType, page.allRecords);
                                    myTable.setMin(i, page.allRecords.elementAt(0));
                                    myTable.setMax(i, page.allRecords.elementAt(page.allRecords.size() - 1));
                                    serializePage(tableName, page, i);
                                    serializePage(tableName, pageAfter, i);
                                } else {
                                    Pages page1 = deserializePage(tableName, i);
                                    Pages page2 = deserializePage(tableName, i + 1);
                                    Record toBeShifted = page2.allRecords.elementAt(0);
                                    page2.allRecords.removeElementAt(0);
                                    page1.allRecords.add(toBeShifted);
                                    Sort(myTable.keyType, page1.allRecords);
                                    myTable.removeMin(i);
                                    myTable.removeMax(i);
                                    myTable.setMax(i, page1.allRecords.elementAt(page1.allRecords.size() - 1));
                                    myTable.setMin(i, page1.allRecords.elementAt(0));
                                    serializePage(tableName, page1, i);
                                    serializePage(tableName, page2, i + 1);
                                }
                            }

                        } catch (Exception er) {
                        }
                    }
                }

            }

        } else {
            boolean removed = false;
            for (int i = 1; i <= myTable.numberOfPages; i++) {
                boolean removedFromthisPage = false;
                Pages currentPage = deserializePage(tableName, i);
                for (int j = 0; j < currentPage.allRecords.size(); j++) {
                    if (matchRecord(currentPage.allRecords.elementAt(i), allTuples)) {
                        currentPage.allRecords.removeElementAt(j);
                        removed = true;
                        removedFromthisPage = true;
                    }
                }
                if (removedFromthisPage) {
                    serializePage(tableName, currentPage, i);
                }

            }
            if (!removed) {
                throw new DBAppException("No records exist with the following criteria");
            }
        }

        // delete from students
        // where id < 50

    }

    public boolean matchRecord(Record a, Vector<Tuple> b) {
        for (int i = 0; i < b.size(); i++) {
            if (a.contains(b.elementAt(i)) == -1) {
                return false;
            }
        }
        return true;

    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {

        String tableName = sqlTerms[0]._strTableName;
        Vector<Tuple> conditions = new Vector<Tuple>();
        Vector<Vector<Record>> satisfied = new Vector<Vector<Record>>();
        Vector<Record> allSatisfied = new Vector<Record>();

        // Check if table name exists
        File tempFile = new File("src\\main\\resources\\" + tableName + ".csv");
        if (tempFile.exists() == false) {
            throw new DBAppException("Table does not exist");
        }

        // Check if table name in conditions match
        // Joins not supported so cannot be different
        for (int i = 1; i < sqlTerms.length; i++) {
            if (!(tableName.equals(sqlTerms[i]._strTableName))) {
                throw new DBAppException("Constraints must be on the same table");
            }
        }

        // Loop to make conditons tuples
        for (int i = 0; i < sqlTerms.length; i++) {
            try {
                Tuple tuple = new Tuple(sqlTerms[i]._strColumnName, sqlTerms[i]._objValue, tableName);
                conditions.add(tuple);

            } catch (Exception e) {
                throw new DBAppException(e.getMessage());
            }
        }

        // Loop to get records matching each condition
        for (int i = 0; i < conditions.size(); i++) {
            Vector<Record> condition = searchForConstraint(tableName, sqlTerms[i]._strOperator,
                    conditions.elementAt(i));
            // Satisfied is a vector that contains vectors that satisfy conditions
            satisfied.add(condition);
        }

        // No condition satisfied
        if (satisfied.isEmpty()) {
            return null;
        } else {
            // Loop on satisfied vector
            // Handle operator cases in arrayOperators : "AND", "OR", "XOR"
            int indx = 0;
            while (indx < satisfied.size()) {
                switch (arrayOperators[0]) {
                    case "AND":
                        for (Record r : satisfied.elementAt(indx)) {
                            if (satisfied.elementAt(indx + 1).contains(r)) {
                                if (!(allSatisfied.contains(r))) {
                                    allSatisfied.add(r);
                                }
                            }
                        }
                        indx = indx + 2;
                        break;
                    case "OR":
                        for (Record r : satisfied.elementAt(indx)) {
                            if (!(allSatisfied.contains(r))) {
                                allSatisfied.add(r);
                            }
                        }
                        indx++;
                        break;
                    case "XOR":
                        for (Record r : satisfied.elementAt(indx)) {
                            if (!(satisfied.elementAt(indx + 1).contains(r))) {
                                if (!(allSatisfied.contains(r))) {
                                    allSatisfied.add(r);
                                }
                            }
                        }
                        indx = indx + 2;
                        ;
                        break;
                    default:
                        for (Record r : satisfied.elementAt(indx)) {
                            allSatisfied.add(r);
                        }
                        indx++;
                }
            }
        }

        // Store result in iterator to return iterator
        Iterator<Record> result = allSatisfied.iterator();
        return result;
    }

    public static Vector<Record> searchForConstraint(String tableName, String operator, Tuple tuple)
            throws DBAppException {
        Vector<Record> result = new Vector<Record>();
        // Load table
        Table myTable = deserializeTable(tableName);
        // Loop on Pages to access all records
        for (int i = 1; i < myTable.numberOfPages + 1; i++) {
            Pages myPage = deserializePage(tableName, i);
            // Loop to access each record in page
            for (int j = 0; j < myPage.allRecords.size(); j++) {
                int index = myPage.allRecords.elementAt(j).contains(tuple);
                // Check if column names exists in table
                if (index != -1) {
                    // Check if record fits required criteria
                    if (compareTo(myPage.allRecords.elementAt(j).tuples.elementAt(index).getType(),
                            myPage.allRecords.elementAt(j).tuples.elementAt(index).getValue(), tuple.getValue(),
                            operator)) {
                        result.add(myPage.allRecords.elementAt(j));
                    }
                } else {
                    throw new DBAppException("Column does not exist in table");
                }
            }
        }
        return result;
    }

    public static boolean compareTo(String keyType, Object a, Object b, String operator) {
        int comparison;
        if (keyType.equals('"' + "java.lang.Integer" + '"')) {
            int minCheck1 = Integer.parseInt(a.toString());
            int minimum1 = Integer.parseInt(b.toString());
            comparison = minCheck1 - minimum1;
        } else if (keyType.equals('"' + "java.lang.Double" + '"')) {
            BigDecimal minCheck1 = new BigDecimal(a.toString());
            BigDecimal minimum1 = new BigDecimal(b.toString());
            comparison = minCheck1.compareTo(minimum1);
        }

        else if (keyType.equals('"' + "java.lang.String" + '"')) {
            String minCheck1 = a.toString();
            String minimum1 = b.toString();
            comparison = minCheck1.compareTo(minimum1);
        } else {
            Date minCheck1 = (Date) a;
            Date minimum1 = (Date) b;
            comparison = minCheck1.compareTo(minimum1);
        }
        switch (operator) {
            case "=":
                return comparison == 0;
            case ">=":
                return comparison >= 0;
            case "<=":
                return comparison <= 0;
            case "<":
                return comparison < 0;
            case ">":
                return comparison > 0;
            case "!=":
                return comparison != 0;
        }
        return false;

    }

    public static void main(String[] args) throws ParseException {

    }
}