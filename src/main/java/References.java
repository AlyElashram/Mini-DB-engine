import java.io.Serializable;

public class References implements Serializable{
    int pageNumber;
    int recordNumber;
    
    public References(int pageName,int recordNumber){
        this.pageNumber=pageName;
        this.recordNumber=recordNumber;

    }
       
    
}