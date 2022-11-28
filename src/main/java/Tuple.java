import java.util.*;
import java.io.*;
import java.math.BigDecimal;

public class Tuple implements Serializable {
    Vector<String> stringValues = new Vector<>();
    Vector<Integer> integerValues = new Vector<>();
    Vector<Date>dateValues = new Vector<>();
    Vector<BigDecimal> doubleValues = new Vector<>();
    private String colName;
    String minimum;
    String maximum;
    private boolean clustering;
    String type;
    int csvLines;

    public boolean isClustering() {
        return clustering;
    }

    transient String[] metaData;

    Tuple(String colName, Object value, String tableName) throws Exception {
        this.colName = colName;

        String line = "";
        String splitBy = ",";
        boolean inserted=false;
        try {
            // parsing a CSV file into BufferedReader class constructor
            BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\" + tableName + ".csv"));
            int number = 0;
            while ((line = br.readLine()) != null) // returns a Boolean value
            {
                number++;
                metaData = line.split(splitBy);// use comma as separator
                String name = '"' + colName + '"';
                this.minimum=metaData[5].substring(1,metaData[5].length()-1);
                this.maximum=metaData[6].substring(1,metaData[6].length()-1);
                
                if (metaData[1].toString().equals(name)) {
                    inserted=true;
                    String type = metaData[2];
                    this.type = type;
                    if(metaData[3].equals('"'+"True"+'"')){
                        this.clustering=true;
                    }
                    if (type.equals('"' + "java.lang.String" + '"')) {
                        if(value.toString().compareTo(minimum)<0||value.toString().compareTo(maximum)>0){
                            throw new Exception("Value to be entered is not within range of accepted values");
                        }
                        stringValues.add(value.toString());
                    }
                    if (type.equals('"' + "java.lang.Integer" + '"')) {
                        try {
                            int min=Integer.parseInt(minimum);
                            int max=Integer.parseInt(maximum);
                            int val=Integer.parseInt(value.toString());

                            if(val<min||val>max){
                                throw new DBAppException("Value to be entered is not within range of accepted values");
                            }
                            integerValues.add(Integer.parseInt(value.toString()));
                        } catch (Exception e) {
                            throw new DBAppException("Integer is not parsable");

                        }
                    }
                    if (type.equals('"' + "java.lang.Double" + '"')) {
                        try {
                            BigDecimal min=new BigDecimal(minimum);
                            BigDecimal max= new BigDecimal(maximum);
                            BigDecimal val=new BigDecimal(value.toString());
                            if(val.compareTo(min)<0||val.compareTo(max)>0){
                                throw new DBAppException("Value to be entered is not within range of accepted values");
                            }
                            doubleValues.add(new BigDecimal(value.toString()));
                        } catch (Exception e) {
                            throw new DBAppException("Double is not parsable");
                        }
                    }
                    if (type.equals('"' + "java.util.Date" + '"')) {
                        dateValues.add((Date)value);
                    
                    }
                    break;
                }



               
                }
                
                if(!inserted){
                    throw new Exception("Column you are trying to insert was not found in the metadata");
                }
            csvLines = number;
        } catch (IOException e) {
            e.printStackTrace();
            
           
        }
        
       

    }

    public String getColName() {
        return colName;
    }

    public Object getValue() {
        if (!(stringValues.isEmpty())) {
            return stringValues.elementAt(0);
        } else if (!(doubleValues.isEmpty())) {
            return doubleValues.elementAt(0);
        } else if (!(integerValues.isEmpty())) {
            return integerValues.elementAt(0);
        } else {
            return !dateValues.isEmpty() ? dateValues.elementAt(0) : null;
        }

    }

    public void setValue(Object newValue) throws DBAppException {
        if (!(stringValues.isEmpty())) {
            stringValues.setElementAt(newValue.toString(), 0);
        } else if (!(doubleValues.isEmpty())) {
            doubleValues.setElementAt(new BigDecimal(newValue.toString()), 0);
        } else if (!(integerValues.isEmpty())) {
            integerValues.setElementAt((Integer) newValue, 0);
        } else {

                dateValues.remove(0);
                dateValues.add((Date)newValue);
           
    }
    }

    public String getType() {
        return type;
    }

}
