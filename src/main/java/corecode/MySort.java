package corecode;

import java.io.Serializable;

public class MySort implements Comparable<MySort>,Serializable {
    private Integer first;
    private Integer second;

    public MySort(Integer first, Integer second) {
        this.first = first;
        this.second = second;
    }

    public Integer getFirst() {
        return first;
    }

    public void setFirst(Integer first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return "first = "+this.first+",second = "+this.second;
    }

    @Override
    public int compareTo(MySort o) {
        if(this.first == o.first){
            return this.second - o.second;
        }else {
            return this.first - o.first;
        }
    }
}
