import java.io.*;
import java.math.*;
import java.text.*;
import java.time.*;
import java.util.*;

public class Grid implements Serializable {
    Hashtable<String, Vector<Range>> colNameRange = new Hashtable<String, Vector<Range>>();
    Vector<Range> arrR = new Vector<Range>();
    String[] colName;
    String minimum, maximum;
    transient String[] metaData;
    Bucket[] gridIndex;

    public Grid(String[] colName, String tableName) throws DBAppException {
        this.colName = colName;
        int n = colName.length;
        gridIndex = new Bucket[10 ^ n];

        String line = "";
        String splitBy = ",";
        try {
            // parsing a CSV file into BufferedReader class constructor
            BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\" + tableName + ".csv"));
            for (int i = 0; i < colName.length; i++) {
                while ((line = br.readLine()) != null) {
                    metaData = line.split(splitBy);// use comma as separator
                    this.minimum = metaData[5].substring(1, metaData[5].length() - 1);
                    this.maximum = metaData[6].substring(1, metaData[6].length() - 1);
                    String type = metaData[2];
                    if (type.equals('"' + "java.lang.String" + '"')) {
                        String min = minimum;
                        String max = maximum;
                        if (min.charAt(2) == '-') {
                            int nmin = Integer.parseInt(min.substring(0, 2));
                            int nmax = Integer.parseInt(max.substring(0, 2));
                            int step = (Integer) (nmax - nmin + 1) / 10;
                            int lowerB = nmin;
                            int upperB = nmin + step;
                            String lowerBB = lowerB + "-0000";
                            String upperBB = upperB + "-9999";
                            while (upperB < nmax) {
                                Range range = new Range(lowerBB, upperBB);
                                lowerB = upperB;
                                upperB = lowerB + step;
                                lowerBB = lowerB + "-0000";
                                upperBB = upperB + "-9999";
                                arrR.add(range);
                            }
                            colNameRange.put(colName[i], arrR);
                            arrR.clear();
                        } else {
                            // Changed code
                            int nmin = min.charAt(0);
                            int nmax = max.charAt(0);
                            // int step = (int) ((nmax - nmin) + 1 / 10);
                            int step = 5;
                            int lowerB = nmin;
                            int upperB = nmin + step;
                            String lowerBB = (Character.toString((char) lowerB) + min.substring(1, min.length()));
                            String upperBB = (Character.toString((char) upperB) + max.substring(1, max.length()));
                            Range range = new Range(lowerBB, upperBB);
                            arrR.add(range);
                            // System.out.println(lowerB + " " + upperB + " " + step + " " + lowerBB + " " +
                            // upperBB);
                            while (upperB < nmax) {
                                System.out.println(lowerBB);
                                System.out.println(lowerBB);
                                lowerB = upperB;
                                upperB = lowerB + step;
                                lowerBB = ((char) lowerB) + min.substring(1, min.length());
                                upperBB = ((char) upperB) + max.substring(1, max.length());
                                range = new Range(lowerBB, upperBB);
                                arrR.add(range);
                            }
                            colNameRange.put(colName[i], arrR);
                            arrR.clear();
                        }

                    }
                    if (type.equals('"' + "java.lang.Integer" + '"')) {
                        int min = Integer.parseInt(minimum);
                        int max = Integer.parseInt(maximum);
                        int step = (Integer) (max - min + 1) / 10;
                        int lowerB = min;
                        int upperB = min + step;

                        while (upperB < max) {
                            Range range = new Range(lowerB, upperB);
                            lowerB = upperB;
                            upperB = lowerB + step;
                            arrR.add(range);

                        }
                        colNameRange.put(colName[i], arrR);
                        arrR.clear();

                    }
                    if (type.equals('"' + "java.lang.Double" + '"')) {
                        BigDecimal min = new BigDecimal(minimum);
                        BigDecimal max = new BigDecimal(maximum);
                        MathContext mc = new MathContext(6);
                        BigDecimal sum = min.add(new BigDecimal("1.000000"), mc);
                        BigDecimal step = (max.subtract(sum, mc)).divide(new BigDecimal("10.000000"),
                                RoundingMode.CEILING);
                        BigDecimal lowerB = min;
                        BigDecimal upperB = min.add(step, mc);

                        while (upperB.compareTo(max) == -1) {
                            Range range = new Range(lowerB, upperB);
                            lowerB = upperB;
                            upperB = lowerB.add(step, mc);
                            arrR.add(range);

                        }
                        colNameRange.put(colName[i], arrR);
                        arrR.clear();

                    }
                    if (type.equals('"' + "java.util.Date" + '"')) {

                        try {
                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                            Date min = format.parse(minimum);
                            Date max = format.parse(maximum);
                            Duration step1 = Duration.between(toLocalDate(min).atStartOfDay(),
                                    toLocalDate(max).atStartOfDay());
                            long step = step1.toDays();
                            Date lowerB = min;
                            Date upperB = max;
                            while (upperB.compareTo(max) == -1) {
                                Range range = new Range(lowerB, upperB);
                                lowerB = upperB;
                                upperB = toDate(toLocalDate(upperB).plusDays(step));
                                arrR.add(range);

                            }
                            colNameRange.put(colName[i], arrR);
                            arrR.clear();

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                }

            }

        } catch (IOException e) {
            e.printStackTrace();

        }

    }

    public Date toDate(LocalDate dateToConvert) {
        return java.sql.Date.valueOf(dateToConvert);
    }

    public LocalDate toLocalDate(Date dateToConvert) {
        return dateToConvert.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

}
