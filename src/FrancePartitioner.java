import org.apache.spark.Partitioner;
import java.io.Serializable;

/**
 * Created by vini on 28/03/16.
 * Essa classe faz o particionamento de acordo com o FRANCE.
 */
public class FrancePartitioner extends Partitioner implements Serializable {

    private int numPartitions;
    public FrancePartitioner(int numeroDeParticoes){
        numPartitions = numeroDeParticoes;

    }


    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
       if (key.toString().equals("")) return 0;
        else
        return Integer.valueOf(key.toString());


    }
}
