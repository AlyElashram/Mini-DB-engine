import java.io.*;
import java.util.*;


public class Pages implements Serializable {
    String tableName;
    String primaryKey;
    int pageSize;
    String keyType;
    Vector<Record> allRecords;

    Pages(Record r) {

        this.pageSize=pageSize();
        this.keyType=r.keyType;
        allRecords=new Vector<>(pageSize);
        this.tableName = r.tableName;
        this.primaryKey = r.clusteringKey;
        allRecords.add(r);
    }



    public void addToPage(Record r) {
        this.allRecords.add(r);
    }

    
    public static int pageSize(){
        Properties prop = new Properties();
        String fileName = "src\\main\\resources\\DBApp.config";
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        try {
            prop.load(is);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));

            }

}

