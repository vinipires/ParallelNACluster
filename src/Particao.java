import java.util.ArrayList;

/**
 * Created by Vinicius.Pires on 18/11/2015.
 */
public class Particao {

    private String name;
    private ArrayList<Cluster> clusters;


    public ArrayList<Cluster> getClusters() {
        return clusters;
    }

    public void setClusters(ArrayList<Cluster> clusters) {
        this.clusters = clusters;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Particao(String name){
        this.name = name;
    }
}
