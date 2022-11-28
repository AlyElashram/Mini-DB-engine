import java.io.Serializable;

public class Range implements Serializable{
    Object lowerB,
    upperB;
    public Range(Object lowerB,Object upperB){
        this.lowerB=lowerB;
        this.upperB=upperB;

    }
}