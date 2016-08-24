import ch.ethz.globis.pht.BitTools;
import ch.ethz.globis.pht.PhTreeVD;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by Vinicius.Pires on 25/08/2015.
 */
public class NAClusterSpark  implements Serializable {

    public static final int MAXDISTANCE = 10000;

    /** Constants for stopping criteria */
    public static final int STOP_EPOCHS = 0;
    public static final int STOP_SAME_CENTROIDS = 1;

    /* This class is used to calculate the stopping criteria */
    private static Threshold threshold = new ConstantEpochsThreshold(20);

    private static int attributesLenght = 2;
    /** index of Attributes to be used */
    private static int[] attributesUsed = { 2, 3 };
    //private static JavaPairRDD<Record, Cluster> recordClusterRDD;

    private static JavaPairRDD<String, String> pIdEstrelaRdd;


    private static  Distance distanceAlgorithm = new EuclideanDistance();
    private static double raFronteira;
    private static double epsilon = 0.000287;

    private static ArrayList<Record> teste = new ArrayList<>();

    private static int primeiroRecordId;

    private static List<String> partitions;
    private static HashMap<Double,String> partitionsMap;
    private static int ultimoRecordId;
    private static double grauAmbiguidadeSoma;

    private static String outputPath;
    private static double sizeClusters;
    private static double sizeRecords;
    private static java.lang.String separator = ";";
    private static String fileName;
    private final Broadcast<List<String>>  partitionsListBroadcast;
    private final Broadcast<HashMap<Double,String>>  partitionsMapBroadCast;
    private static JavaSparkContext sc;

    public NAClusterSpark(String inputPath, String outputPath, String partitionFileName) throws ClassNotFoundException {
        this.outputPath = outputPath;
        fileName = inputPath;

        SparkConf config = new SparkConf().setMaster("spark://mnmaster:7077").setAppName("Parallel NACluster").registerKryoClasses(new Class<?>[]{Class.forName("Cluster"), Class.forName("NAClusterSpark")});
         sc = new JavaSparkContext(config);

        JavaRDD<String> conjuntoCatalogos = sc.textFile(fileName).persist(StorageLevel.MEMORY_AND_DISK());

        partitions= sc.textFile(partitionFileName).collect();
        partitionsListBroadcast = sc.broadcast(partitions);




        // O partitionsMap é criado para ajudar na verificação se o objeto é da fronteira. Ele é usando durante a função readRecord();
        partitionsMap = new HashMap<>();
        partitionsMap.put(0.0, "p" + (partitionsListBroadcast.value().size() + 1) + "p1");
        System.out.println("partitionsMap.get(0.0) = " + partitionsMap.get(0.0));

        if (partitionsListBroadcast.value().size() > 1){
        for (int a = 0; a < partitionsListBroadcast.value().size(); a++) {

            partitionsMap.put(Double.valueOf(partitionsListBroadcast.value().get(a)), "p"+(a+2)+"p"+(a+1));
            System.out.println(partitionsListBroadcast.value().get(a) +" = "  + partitionsMap.get(Double.valueOf(partitionsListBroadcast.value().get(a))));

        }
        partitionsMap.put(360.0, "p" + (partitionsListBroadcast.value().size() + 1) + "p1");
        System.out.println("partitionsMap.get(360.0) = " + partitionsMap.get(360.0));}
        else {

            partitionsMap.put(360.0, "p2p1");
            System.out.println("partitionsMap.get(360.0) = " + partitionsMap.get(360.0));
            partitionsMap.put(Double.valueOf(partitionsListBroadcast.value().get(0)), "p1p2");
            System.out.println("partitionsMap.get("+ partitionsListBroadcast.value().get(0)+") = " + partitionsMap.get(Double.valueOf(partitionsListBroadcast.value().get(0))));

        }
        partitionsMapBroadCast = sc.broadcast(partitionsMap);


        partitionsMap = partitionsMapBroadCast.value();
        partitions = partitionsListBroadcast.value();

        // Aqui está iniciando o particionamento.
         // Primeiramente, pegamos o RDD de string contendo  o conjuntoCatalogos e
         // a partir dele criamos um novo data set do tipo pId-Estrela particionado.


        int numeroParticoes = partitionsListBroadcast.value().size() +1;
        FrancePartitioner particionamento = new FrancePartitioner(numeroParticoes);

        pIdEstrelaRdd = conjuntoCatalogos.mapToPair(line ->{
            String[] words = line.split(";");
            Double ra = Double.parseDouble(words[1]);
            String partitionId = "";
            if (partitionsListBroadcast.value().size()>1){
            for (int i = 0; i < partitionsListBroadcast.value().size(); i++) {
                if (i == partitionsListBroadcast.value().size() -1) {
                    if (ra < 361.0 && ra >= Double.parseDouble(partitionsListBroadcast.value().get(i))) {
                        partitionId = String.valueOf(i+1);
                    }
                    if(ra < Double.parseDouble(partitionsListBroadcast.value().get(i)) && ra >= Double.parseDouble(partitionsListBroadcast.value().get(i-1))){
                        partitionId = String.valueOf(i);
                    }
                }
                else{
                    if (i == 0) {
                        if (ra < Double.parseDouble(partitionsListBroadcast.value().get(i)) && ra >= -1.0) {
                            partitionId = String.valueOf(i);
                        }
                        if (ra < Double.parseDouble(partitionsListBroadcast.value().get(i+1)) && ra >= Double.parseDouble(partitionsListBroadcast.value().get(i))) {
                            partitionId = String.valueOf(i+1);
                        }

                    }else if (i > 0 && i < partitionsListBroadcast.value().size() - 1) {

                        if (ra < Double.parseDouble(partitionsListBroadcast.value().get(i)) && ra >= Double.parseDouble(partitionsListBroadcast.value().get(i - 1))) {
                            partitionId = String.valueOf(i);
                        }
                        if (ra < Double.parseDouble(partitionsListBroadcast.value().get(i + 1)) && ra >= Double.parseDouble(partitionsListBroadcast.value().get(i))) {
                            partitionId =  String.valueOf(i);
                        }
                    }
                }
            }}
            else{
                if (ra < 361.0 && ra >= Double.parseDouble(partitionsListBroadcast.value().get(0))) {
                    partitionId = "1";
                }
                else{
                    partitionId = "0";
                }
            }
            return new Tuple2<>(partitionId,"p_" + partitionId + ";"+line);
        }).partitionBy(particionamento).persist(StorageLevel.MEMORY_AND_DISK());
        conjuntoCatalogos.unpersist();


        List<Partition> particoes = pIdEstrelaRdd.partitions();
        System.out.println("pIdEstrelaRdd particionado = " + pIdEstrelaRdd.partitions().size());

        for (Partition particao : particoes) {
            System.out.println(particao.index());
        }


    }



    public <U> void run() throws IOException, ClassNotFoundException {


        /**
         * Criando o Par do tipo (Particao,Cluster) atraves da primeira Rodada do NACluster
         */


        JavaPairRDD<String, Cluster> clustersPairRDD = pIdEstrelaRdd.mapPartitionsToPair(lines -> createClustersPair(lines)).persist(StorageLevel.MEMORY_AND_DISK());
        pIdEstrelaRdd.unpersist();


        List<Partition> particoes = clustersPairRDD.partitions();
        System.out.println("clustersPairRDDPartitionsSize = " + clustersPairRDD.partitions().size());

        for (Partition particao : particoes) {

            System.out.println(particao.index());
        }




        System.out.println("clustersPairRDDPartitionsSize = " + clustersPairRDD.partitions().size());
        /**
         * Separando os clusters da Fronteira dos clusters de Fora da Fronteira
         */

        JavaPairRDD<String, Cluster> clustersPairForaRDD = clustersPairRDD.filter(clusterPair -> selecionaFora(clusterPair)).persist(StorageLevel.MEMORY_AND_DISK());

        JavaPairRDD<String, Cluster> clustersPairFronteiraRDD = clustersPairRDD.filter(clusterPair -> selecionaFronteira(clusterPair));

        clustersPairRDD.unpersist();
        List<Partition> particoesFora = clustersPairForaRDD.partitions();
        System.out.println("clustersPairForaRDD = " + clustersPairForaRDD.partitions().size());

        for (Partition particao : particoesFora) {

            System.out.println(particao.index());
        }


        /**
         * Criando uma tupla <Particao,List<Cluster>> para cada tupla <Particao,Cluster>
         *     Ou seja, cada List<Cluster> contem apenas um elemento;
         *     Para que depois seja possivel fazer o reduceByKey e criar a tupla
         *     <Particao,List<Cluster> contendo todos os clusters da particao correspondente.
         */
        JavaPairRDD<String,List<Cluster>> rddFronteiraClusterList = clustersPairFronteiraRDD.mapToPair((clusterPair -> {

            List<Cluster> clusterList = new ArrayList<Cluster>();
            clusterList.add(clusterPair._2());

            return new Tuple2<String, List<Cluster>>(clusterPair._1(), clusterList);
        }));

        rddFronteiraClusterList = rddFronteiraClusterList.reduceByKey((clustersList1, clustersList2) -> {
            clustersList1.addAll(clustersList2);
            return clustersList1;
        });


        System.out.println("rddFronteiraClusterList = " + rddFronteiraClusterList.partitions().size());

        List<Partition> particoesFronteira = rddFronteiraClusterList.partitions();
        for (Partition particao : particoesFronteira) {

            System.out.println(particao.toString());
        }

        /**
         * Agora o RDD esta preparado para realizar o NACluster somente nos clusters
         * influenciados pela fronteira.
         */





        rddFronteiraClusterList =  rddFronteiraClusterList.mapValues(clusters -> naclusterReduce2(clusters));




        /**
         * O RDD deve voltar para o seu formato original. Ao inves da tupla <Particao,List<Cluster>>
         *     voltaremos para <Particao,Cluster> atraves do mapPartitiosToPair abaixo.
         */
        clustersPairFronteiraRDD = rddFronteiraClusterList.mapPartitionsToPair(tuple2Iterator -> {
            List<Tuple2<String, Cluster>> stringClusterPair = new ArrayList<Tuple2<String, Cluster>>();

            while (tuple2Iterator.hasNext()) {
                Tuple2<String, List<Cluster>> tuplaClusterList = tuple2Iterator.next();

                List<Cluster> clusterList = tuplaClusterList._2();
                for (int i = 0; i < clusterList.size(); i++) {
                    stringClusterPair.add(new Tuple2<String, Cluster>(tuplaClusterList._1(), clusterList.get(i)));
                }
            }
            return stringClusterPair;
        });

        /**
         * Com o RDD da fronteira de volta ao seu formato original, podemos fazer o union dele com
         * o RDD que contem os clusters de fora da fronteira. E assim, termos o RDD resultante do
         * Parallel NACluster.
         */
        clustersPairRDD = clustersPairForaRDD.union(clustersPairFronteiraRDD);

        List<Partition> particoes3 = clustersPairRDD.partitions();
        System.out.println("clustersPairRDDAposUnion = " + clustersPairRDD.partitions().size());

        for (Partition particao : particoes3) {

            System.out.println(particao.toString());
        }

        /**
         * Atraves deste combineByKey, podemos contar a quantidade de clusters corretos e a quantidade
         * total de clusters gerados pelo Parallel NACluster. Com essas informacoes, podemos calcular
         * a qualidade da clusterizacao.
         */
        JavaPairRDD<String, ClustersCount> contagemDeClusters = clustersPairRDD.combineByKey(cluster -> {
                    ClustersCount clustersCount = verificarClusterCorreto(cluster);
                    return clustersCount;
                }, (clustersCount1, cluster) -> { // Merge a Value

                    ClustersCount clustersCount = verificarClusterCorreto(cluster);
                    clustersCount.clustersCorretos += clustersCount1.clustersCorretos;
                    clustersCount.clustersTotal += clustersCount1.clustersTotal;

                    return clustersCount;
                },
                (clustersCount, clustersCount1) -> { // Merge two combiners
                    clustersCount.clustersCorretos += clustersCount1.clustersCorretos;
                    clustersCount.clustersTotal += clustersCount1.clustersTotal;
                    return clustersCount;
                }
        );




        /*Provides access to configuration parameters*/
        Configuration conf = new Configuration();
        /*Creating Filesystem object with the configuration*/
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Path output = new Path(outputPath);
        /*Check if output path (args[1])exist or not*/
        if (fs.exists(output)){
        /*If exist delete the output path*/
            fs.delete(output,true);
        }

        contagemDeClusters.saveAsTextFile(outputPath);
        List<Tuple2<String, ClustersCount>> particaoClustersCount = contagemDeClusters.collect();


        double clustersCorretos = 0;
        double clustersTotal = 0;
        for (Tuple2<String, ClustersCount> stringClustersCountTuple2 : particaoClustersCount) {
            clustersCorretos += stringClustersCountTuple2._2().clustersCorretos;
            clustersTotal += stringClustersCountTuple2._2().clustersTotal;
            System.out.println("Particao = " + stringClustersCountTuple2._1() + ", Clusters Corretos = " + stringClustersCountTuple2._2().clustersCorretos + ", Clusters Errados =" + (stringClustersCountTuple2._2().clustersTotal - stringClustersCountTuple2._2().clustersCorretos));

        }

        double precision = clustersCorretos / clustersTotal;
        System.out.println("clustersCorretos = " + clustersCorretos);
        System.out.println("clustersTotal = " + clustersTotal);

        System.out.println("Precision = " + precision);
        sc.close();


    }



    private  ClustersCount verificarClusterCorreto(Cluster cluster) {
        ArrayList<Record> recordsCluster;
        Integer clustersCorretos = 0;
        int sizeCluster;
        recordsCluster = cluster.getRecords();

        sizeCluster = recordsCluster.size();
        boolean certo = true;
        // int id1 = recordsCluster.get(0).getId();
        Record record1 = recordsCluster.get(0);

        if (sizeCluster == 1 && record1.getQtdClusterReal() == 1) {
            clustersCorretos++;
        } else {
            for (int i = 1; i < sizeCluster; i++) {
                if (record1.getId() != recordsCluster.get(i).getId()) certo = false;
            }
            if (certo) {
                if (sizeCluster == record1.getQtdClusterReal())
                    clustersCorretos++;
                //else {
                //clustersErrados++;
                //}
            } //else clustersErrados++;
        }

        ClustersCount clustersCount = new ClustersCount(clustersCorretos, 1);

        return clustersCount;
    }


    private ClustersCount contarClusters(List<Cluster> clusters) {
        ArrayList<Record> recordsCluster;
        Integer clustersCorretos = 0;
        //int clustersErrados = 0;

        int sizeCluster;

        for (Cluster cluster : clusters) {
            recordsCluster = cluster.getRecords();

            sizeCluster = recordsCluster.size();
            boolean certo = true;
            // int id1 = recordsCluster.get(0).getId();
            Record record1 = recordsCluster.get(0);

            if (sizeCluster == 1 && record1.getQtdClusterReal() == 1) {
                clustersCorretos++;
            } else {
                for (int i = 1; i < sizeCluster; i++) {
                    if (record1.getId() != recordsCluster.get(i).getId()) certo = false;
                }
                if (certo) {
                    if (sizeCluster == record1.getQtdClusterReal())
                        clustersCorretos++;
                    //else {
                    //clustersErrados++;
                    //}
                } //else clustersErrados++;
            }


        }

        ClustersCount clustersCount = new ClustersCount(clustersCorretos, clusters.size());

        return clustersCount;
    }

    private Iterable<Tuple2<String, Cluster>> createClustersPair(Iterator<Tuple2<String, String>> lines) {
        //HashMap<Double, String> partitionsMap = new HashMap<>();




        ArrayList<Tuple2<String,Cluster>> clustersPair = new ArrayList<>();
        if (lines.hasNext()) {
            Collection<Record> records = new ArrayList<>();
            PhTreeVD<Cluster> phtree = new PhTreeVD<>(attributesLenght);

            List<Record> recordsFronteira = new ArrayList<>();


            // leitura do primeiro record
            //Feita para capturar o id do primeiro record para ser usado no calculo do recall


            Record r = readRecord(lines.next()._2());
            records.add(r);

       /* System.out.println("r.getDataString() = " + r.getDataString());
        System.out.println("r.getPartitionName() = " + r.getPartitionName());
        System.out.println("r.getIdCatalog() = " + r.getIdCatalog());
        System.out.println("r.getQtdClusterReal() = " + r.getQtdClusterReal());
        System.out.println("r.getRa() = " + r.getRa());
        System.out.println("r.getId() = " + r.getId());*/

            primeiroRecordId = r.getId();
            if (r.isFronteira()) {
                recordsFronteira.add(r);
                //    teste.add(r);
            }


            if (r.getIdCatalog() == 1) {


                Cluster cluster = new Cluster(attributesLenght);
                cluster.setPartitionName(r.getPartitionName());
                cluster.addRecord(r);
                cluster.calculateCentroid();
                cluster.removeRecord(r);


                phtree.put(cluster.getCentroid(), cluster);

                //recordClusterList.add(new Tuple2<>(r,cluster));


            }


            // leitura dos demais records
            while (lines.hasNext()) {

                r = readRecord(lines.next()._2());
                records.add(r);
                assert r != null;

            /*System.out.println("r.getDataString() = " + r.getDataString());
            System.out.println("r.getPartitionName() = " + r.getPartitionName());
            System.out.println("r.getIdCatalog() = " + r.getIdCatalog());
            System.out.println("r.getQtdClusterReal() = " + r.getQtdClusterReal());
            System.out.println("r.getRa() = " + r.getRa());
            System.out.println("r.getId() = " + r.getId());*/

                if (r.isFronteira()) {
                    recordsFronteira.add(r);
                    //System.out.println("fora do read r.getPartitionName() = " + r.getPartitionName() + " " + r.getDataString());
                    //teste.add(r);
                }


                if (r.getIdCatalog() == 1) {


                    Cluster cluster = new Cluster(attributesLenght);
                    cluster.addRecord(r);
                    cluster.calculateCentroid();
                    cluster.removeRecord(r);
                    cluster.setPartitionName(r.getPartitionName());


                    phtree.put(cluster.getCentroid(), cluster);

                    //recordClusterList.add(new Tuple2<>(r,cluster));


                }
                //else recordClusterList.add(new Tuple2<>(r,null));
            }

            ultimoRecordId = r.getId();


            sizeRecords = records.size();


            /** Aqui fica a itera��o do algoritmo, precisa ser automatizada
             * O problema � que quando eu uso o c�digo abaixo, todos os clusters
             *  ficam zerados ao utilizar a Constant Ephocs*/

        /*while (!threshold.isDone(phtree)) {
            x++;


            System.out.println("phtree.size() = " + phtree.size());
               if(threshold.getCurrentIteration() > 1)
                phtree = resetRecordsFromClusters(phtree);
                phtree = nacluster2(phtree, records);


        }*/

            for (int i = 0; i < 1; i++) {

                if (i > 0) {
                    phtree = resetRecordsFromClusters(phtree);
                }
                phtree = nacluster2(phtree, records);

            }

            phtree = sciBoundary(recordsFronteira, phtree);


            sizeClusters = phtree.size();


            PhTreeVD.PVDIterator<Cluster> iter = phtree.queryExtent();
            while (iter.hasNext()) {
                Cluster cluster = iter.next().getValue();
                clustersPair.add(new Tuple2(cluster.getPartitionName(), cluster));
            }


        }
        return clustersPair;


    }

    private List<Cluster> naclusterReduce2(List<Cluster> clusters) {

        Collection<Record> records = new ArrayList<>();
        PhTreeVD<Cluster> phtree = new PhTreeVD<>(attributesLenght);





            for (Cluster cluster : clusters) {
                records.addAll(cluster.getRecords());




            }


        for (Record record : records) {
            if (record.getIdCatalog() == 1) {


                Cluster cluster = new Cluster(attributesLenght);
                cluster.addRecord(record);
                cluster.calculateCentroid();
                cluster.removeRecord(record);
                cluster.setPartitionName(record.getPartitionName());


                phtree.put(cluster.getCentroid(), cluster);

                //recordClusterList.add(new Tuple2<>(r,cluster));


            }
        }






        /** Aqui fica a iteracao do algoritmo, precisa ser automatizada */
         for (int i = 0; i < 2; i++) {

             if (i>0) phtree = resetRecordsFromClusters(phtree);
        phtree = nacluster2(phtree, records);

        }



        PhTreeVD.PVDIterator<Cluster> iter = phtree.queryExtent();




        clusters.clear();
        for (; iter.hasNext();) {
            clusters.add(iter.next().getValue());

        }




        return clusters;



    }


    private Boolean selecionaFronteira(Tuple2<String,Cluster> clusterPair) {
        return clusterPair._2().isFronteira();
    }

    private Boolean selecionaFora(Tuple2<String,Cluster> clusterPair) {
        return !clusterPair._2().isFronteira();
    }

    private Iterable<Cluster> createClusters(Iterator<String> lines) {
        List<Cluster> clusters = new ArrayList<>();

        if(lines.hasNext()) {

            Collection<Record> records = new ArrayList<>();
            PhTreeVD<Cluster> phtree = new PhTreeVD<>(attributesLenght);

            List<Record> recordsFronteira = new ArrayList<>();

            /** leitura do primeiro record
             * Feita para capturar o id do primeiro record para ser usado no calculo do recall
             */


            Record r = readRecord(lines.next());
            records.add(r);

            primeiroRecordId = r.getId();
            if (r.isFronteira()) {
                recordsFronteira.add(r);
                teste.add(r);
            }


            if (r.getIdCatalog() == 1) {


                Cluster cluster = new Cluster(attributesLenght);
                cluster.addRecord(r);
                cluster.calculateCentroid();
                cluster.removeRecord(r);
                cluster.setPartitionName(r.getPartitionName());


                phtree.put(cluster.getCentroid(), cluster);

                //recordClusterList.add(new Tuple2<>(r,cluster));


            }


            /** leitura dos demais records */
            while (lines.hasNext()) {

                r = readRecord(lines.next());
                records.add(r);
                assert r != null;
                if (r.isFronteira()) {
                    recordsFronteira.add(r);
                    teste.add(r);
                }


                if (r.getIdCatalog() == 1) {


                    Cluster cluster = new Cluster(attributesLenght);
                    cluster.addRecord(r);
                    cluster.calculateCentroid();
                    cluster.removeRecord(r);
                    cluster.setPartitionName(r.getPartitionName());


                    phtree.put(cluster.getCentroid(), cluster);

                    //recordClusterList.add(new Tuple2<>(r,cluster));


                }
                //else recordClusterList.add(new Tuple2<>(r,null));
            }

            ultimoRecordId = r.getId();


            sizeRecords = records.size();


            /** Aqui fica a iteracao do algoritmo, precisa ser automatizada
             * O problema eh que quando eu uso o codigo abaixo, todos os clusters
             *  ficam zerados ao utilizar a Constant Ephocs*/

        /*while (!threshold.isDone(phtree)) {
            x++;


            System.out.println("phtree.size() = " + phtree.size());
               if(threshold.getCurrentIteration() > 1)
                phtree = resetRecordsFromClusters(phtree);
                phtree = nacluster2(phtree, records);


        }*/

            for (int i = 0; i < 2; i++) {

                if (i > 0) {
                    phtree = resetRecordsFromClusters(phtree);
                }
                phtree = nacluster2(phtree, records);

            }

            phtree = sciBoundary(recordsFronteira, phtree);


            sizeClusters = phtree.size();




            PhTreeVD.PVDIterator<Cluster> iter = phtree.queryExtent();
            while (iter.hasNext()) clusters.add(iter.next().getValue());

            /***
             * Aqui ja podemos fazer o lance da fronteira
             *
             * Para cada objeto da fronteira, procurar pelos centroides candidatos. Para cada centroide
             * encontrado, pegar seus objetos e procurar para cada um deles (dos objetos) os centroides
             * candidatos e assim sucessivamente.
             * Assim eu vou encontrando o caminho
             */

        }

        return clusters;



    }

    private PhTreeVD<Cluster> sciBoundary(List<Record> recordsFronteira, PhTreeVD<Cluster> phtree) {


        for (Record record : recordsFronteira) {
            Iterator<PhTreeVD.PVDEntry<Cluster>> results = rangeQuery(
                    phtree, record.getData(), epsilon);

            Cluster clusterInfluenciado;

            for (; results
                    .hasNext();) {
                clusterInfluenciado = results.next().getValue();
                if(!clusterInfluenciado.isFronteira()){
                    clusterInfluenciado.setFronteira(true);
                    clusterInfluenciado.setPartitionName(record.getPartitionName());
                    phtree = searchClustersInfluenciados(phtree, clusterInfluenciado);
                }

            }
        }
        return phtree;
    }

    private PhTreeVD<Cluster> searchClustersInfluenciados(PhTreeVD<Cluster> phtree, Cluster clusterInfluenciado) {

        String partitionName = clusterInfluenciado.getPartitionName();

        ArrayList<Record> recordsInfluenciados = clusterInfluenciado.getRecords();

        for (Record recordInfluenciado : recordsInfluenciados) {
            /**
             *  O raciocinio aqui é que se o record já é da fronteira, já foi feito um
             * searchClustersInfluenciados com ele. Isso solucionou
             */

            if(!recordInfluenciado.isFronteira()) {
                recordInfluenciado.setFronteira(true);
                recordInfluenciado.setPartitionName(partitionName);

                Iterator<PhTreeVD.PVDEntry<Cluster>> results = rangeQuery(
                        phtree, recordInfluenciado.getData(), epsilon);

                for (; results
                        .hasNext(); )
                    if (!results.next().getValue().isFronteira()) {
                        clusterInfluenciado.setFronteira(true);
                        clusterInfluenciado.setPartitionName(partitionName);
                        phtree = searchClustersInfluenciados(phtree, clusterInfluenciado);
                    }
            }
        }
        return phtree;
    }

    private PhTreeVD<Cluster> nacluster2(PhTreeVD<Cluster> centroidsTree, Collection<Record> records) {

        grauAmbiguidadeSoma = 0;
        double currentDistance;
        double distance;
        PhTreeVD.PVDEntry<Cluster> currentCentroid = null;
		/* records of current catalog */
        boolean existCentroid;
        int MAXDISTANCE = 10000;


        for (Record record : records) {
            //   x++;
            currentDistance = MAXDISTANCE;

            /**
             * find the closest centroid for this record
             **/
            existCentroid = false;
            /**
             * search for centroids in radius 0.003 from the
             * position of the record. centroidsTree ia a PHTree
             */

            Iterator<PhTreeVD.PVDEntry<Cluster>> results = rangeQuery(
                    centroidsTree, record.getData(), 0.003);
            /**
             * Why the hashmap is <String,Cluster> not
             * <Long[],Cluster>? Because each time we call
             * rangeQuery function, the centroidLong return has a
             * pointer to an different java object, even if they
             * have the same value in another function call.
             */
            HashMap<Cluster, Double> centroidsCandidatesDistance = new HashMap<>();
            PhTreeVD.PVDEntry<Cluster> clusterCandidate;


            for (; results
                    .hasNext(); ) {
                existCentroid = true;
                clusterCandidate = results.next();

                distance = distanceAlgorithm.calculate(
                        record.getData(), clusterCandidate.getKey());

                /**
                 * search for the shortest distance between the
                 * current record and the current centroid
                 */

                if (currentDistance >= distance) {
                    /**
                     * store the centroid with the shortest distance
                     * from the record
                     */
                    currentCentroid = clusterCandidate;

                    currentDistance = distance;
                }
                /**
                 * each centroid and its distance returned from
                 * range query are inserted in
                 * centroidsCandidatesDistance hashmap
                 */
                centroidsCandidatesDistance.put(
                        clusterCandidate.getValue(), distance);
            }

            results = null;

            int quantidadeCandidatos = centroidsCandidatesDistance.size();
            grauAmbiguidadeSoma += quantidadeCandidatos -1;



            if (existCentroid) {

                //assert currentCentroid != null;
                Cluster currentCluster = currentCentroid.getValue();

                /**
                 * Verifica se n?o existe registro do mesmo cat?logo
                 * associado ? esse cluster
                 */
                if (!currentCluster.containsCatalogPoint(record.getIdCatalog())) {
                                /*
								 * add this record if not exist a record from
								 * the same catalog in this cluster
								 */


                    currentCluster.addRecord(record);
                    currentCluster.calculateCentroid();


								/*
								 * once a record was inserted in the cluster,
								 * and the centroid position changed (when we
								 * call the calculateCentroid function, we need
								 * of the newCentroid value
								 */

								/*
								 * When we have a new centroid value, we must to
								 * delete the previous centroid from the phtree
								 * and the clustersMap and add a new centroid
								 * value in them
								 */


                    if ((centroidsTree.contains(currentCluster.getCentroid())) && (!centroidsTree.get(currentCluster.getCentroid()).equals(currentCluster))) {
                        Cluster oldCluster = centroidsTree.get(currentCluster.getCentroid());

                        System.out.println("o centroide novo ja existe 0");

                        System.out.println("centroides antes " + Arrays.toString(currentCluster.getCentroid()) + "e " + Arrays.toString(oldCluster.getCentroid()));
                        System.out.println("Records old cluster1 : ");
                        ArrayList<Record> recordsOld = oldCluster.getRecords();
                        for (Record aRecordsOld : recordsOld) {
                            System.out.print(aRecordsOld.getId() + " ");
                        }

                        System.out.println("Records current cluster : ");
                        ArrayList<Record> recordsCurrent = currentCluster.getRecords();
                        for (Record aRecordsCurrent : recordsCurrent) {
                            System.out.print(aRecordsCurrent.getId() + " ");
                        }
                        System.out.println("CentroidesTree antes do antes" + centroidsTree.size());
                        centroidsTree.remove(oldCluster.getCentroid());
                        Record oldRecord = oldCluster.getCatalogPoint(record.getIdCatalog());
                        oldCluster.removeRecord(oldRecord);


                        oldCluster.addRecord(record);

                        currentCluster.removeRecord(record);
                        currentCluster.addRecord(oldRecord);
                        currentCluster.calculateCentroid();


                        oldCluster.calculateCentroid();
                        System.out.println("CentroidesTree antes " + centroidsTree.size());
                        centroidsTree.put(currentCluster.getCentroid(), currentCluster);
                        centroidsTree.put(oldCluster.getCentroid(), oldCluster);


                        System.out.println("novos centroids " + Arrays.toString(currentCluster.getCentroid()) + "e " + Arrays.toString(oldCluster.getCentroid()));
                        //System.out.println("records oldC " + oldCluster.getRecords().get(0).getId() + " e " + oldCluster.getRecords().get(1).getId()+ " e records currentC " + "records oldC " +currentCluster.getRecords().get(0).getId() + " e " +currentCluster.getRecords().get(1).getId());
                        System.out.println("CentroidesTree depois " + centroidsTree.size());
                    } else {


                        int tamanhoantes = centroidsTree.size();
                        if ((centroidsTree.contains(currentCluster.getCentroid())) && (!centroidsTree.get(currentCluster.getCentroid()).equals(currentCluster))) {
                            System.out.println("o centroide novo ja existe 1");
                            //qtdCentroideNovo++;
                        }

                        //System.out.println("Removendo " + Arrays.toString(currentCentroid.getKey()));
                        //System.out.println("Inserindo " + Arrays.toString(currentCluster.getCentroid()));

                        centroidsTree.remove(currentCentroid.getKey());
                        // clustersMap.remove(currentCentroid.getValue());
                        centroidsTree.put(currentCluster.getCentroid(),
                                currentCluster);

                        int tamanhodepois = centroidsTree.size();
                        if (tamanhoantes != tamanhodepois) {
                            System.out.println(" Centroide " + Arrays.toString(currentCluster.getCentroid()) + " existe novo? " + centroidsTree.contains(currentCluster.getCentroid()) + " e o velho? " + Arrays.toString(currentCentroid.getKey()) + " " + centroidsTree.contains(currentCentroid.getKey()));
                            System.out.println(currentCluster.getRecords().get(0).getId());
                        }
                    }
                } else {
                    /**
                     * Se existe registro do mesmo cat?logo
                     * associado a esse cluster
                     */

                    centroidsTree = searchCentroid2(record,
                            centroidsCandidatesDistance,
                            record.getIdCatalog(), centroidsTree);
                }
            } else {

                /**
                 * If not exist centroid in the range query from the
                 * current record create a new cluster and add this
                 * record in that
                 */

                int tamanhoantes = centroidsTree.size();

                Cluster currentCluster = new Cluster(attributesLenght);
                currentCluster.addRecord(record);
                currentCluster.calculateCentroid();
                currentCluster.setPartitionName(record.getPartitionName());


                //System.out.println("Inserindo " + Arrays.toString(currentCluster.getCentroid()));

                centroidsTree.put(currentCluster.getCentroid(),
                        currentCluster);


                int tamanhodepois = centroidsTree.size();
                if (tamanhoantes == tamanhodepois) {
                    System.out.println(" Centroide " + Arrays.toString(currentCluster.getCentroid()) + " existe1? " + centroidsTree.contains(currentCluster.getCentroid()));
                }
                //clustersMap.put(newCentroid, currentCluster);

            }


        }

        return centroidsTree;
    }

    private PhTreeVD<Cluster> searchCentroid2(Record record, HashMap<Cluster, Double> centroidsCandidatesDistance, int idCatalog, PhTreeVD<Cluster> centroidsTree) {

        switch (centroidsCandidatesDistance.size()) {
            case 0:
			/* criando cluster, pois n?o tem centroide candidato */

                Cluster currentCluster = new Cluster(attributesLenght);

                currentCluster.addRecord(record);
                currentCluster.calculateCentroid();
                currentCluster.setPartitionName(record.getPartitionName());
                int tamanhoantes = centroidsTree.size();

                //clustersCollection.add(currentCluster);

                //System.out.println("Inserindo " + Arrays.toString(currentCluster.getCentroid()));

                centroidsTree.put(currentCluster.getCentroid(),
                        currentCluster);




                int tamanhodepois = centroidsTree.size();
                if (tamanhoantes == tamanhodepois) {
                    System.out.println(" Centroide " + Arrays.toString(currentCluster.getCentroid()) + " existe2? " + centroidsTree.contains(currentCluster.getCentroid()));
                }

                break;

            default:
                Set<Cluster> centroidsIds = centroidsCandidatesDistance.keySet();
                double currentDistance = 10000;
                Cluster currentCentroid = null;

			/* performs a seach in the candidates list */
                for (Cluster centroid : centroidsIds) {

                    if (centroidsCandidatesDistance.get(centroid) < currentDistance) {
					/* find the closest centroid */
                        currentDistance = centroidsCandidatesDistance.get(centroid);

                        currentCentroid = centroid;
                    }
                }
                currentCluster = currentCentroid;

                //assert currentCluster != null;
                if (!currentCluster.containsCatalogPoint(idCatalog)) {

                    double[] currentCentroidDouble = convertToDoubleArray(currentCentroid.getCentroidLongBits());

                    currentCluster.addRecord(record);

                    currentCluster.calculateCentroid();
                    //long[] newCentroid = currentCluster.getCentroidLongBits();
                    if ((centroidsTree.contains(currentCluster.getCentroid())) && (!centroidsTree.get(currentCluster.getCentroid()).equals(currentCluster)) ){
                        System.out.println("o centroide novo ja existe 2");
                        // qtdCentroideNovo++;
                        Cluster oldCluster =  centroidsTree.get(currentCluster.getCentroid());
                        centroidsTree.remove(oldCluster.getCentroid());

                        Record oldRecord = oldCluster.getCatalogPoint(idCatalog);

                        oldCluster.removeRecord(oldRecord);
                        oldCluster.addRecord(record);
                        currentCluster.removeRecord(record);
                        currentCluster.addRecord(oldRecord);
                        currentCluster.calculateCentroid();


                        //System.out.println(Math.(36.991940+36.991684, 2));
                        oldCluster.calculateCentroid();
                        centroidsTree.put(currentCluster.getCentroid(), currentCluster);
                        centroidsTree.put(oldCluster.getCentroid(), oldCluster);



                        System.out.println("novos centroids " + Arrays.toString(currentCluster.getCentroid()) + "e " +  Arrays.toString(oldCluster.getCentroid()));
                        //System.out.println("records oldC " + oldCluster.getRecords().get(0).getId() + " e " + oldCluster.getRecords().get(1).getId()+ " e records currentC " + "records oldC " +currentCluster.getRecords().get(0).getId() + " e " +currentCluster.getRecords().get(1).getId());
                    }else{

                        /**
                         * ATEN��O NESSA PARTE, PODE HAVER ALGUM PROBLEMA
                         *
                         * Acontece que n�o existia objeto do mesmo cat�logo de record no cluster,
                         * calculou-se o centr�ide ao adicionar ele no cluster
                         * e n�o existe nenhum centr�ide com a mesma posi��o
                         */

                        tamanhoantes = centroidsTree.size();
                        //System.out.println("Removendo " + Arrays.toString(currentCentroidDouble));
                        //System.out.println("Inserindo " + Arrays.toString(currentCluster.getCentroid()));

                        centroidsTree.remove(currentCentroidDouble);

                        centroidsTree.put(currentCluster.getCentroid(),
                                currentCluster);


                        tamanhodepois = centroidsTree.size();
                        if (tamanhoantes != tamanhodepois) {
                            System.out.println(" Centroide " + Arrays.toString(currentCluster.getCentroid()) + " existe novo? " + centroidsTree.contains(currentCluster.getCentroid())+ " e o velho? " + Arrays.toString(currentCentroidDouble) + " " + centroidsTree.contains(currentCentroidDouble));
                            System.out.println(currentCluster.getRecords().get(0).getId() );
                        }
                    }
                } else {
                    /**
                     * remove the object oldRecord from the cluster currentCluster,
                     * insert record in this cluster, and search another cluster for
                     * oldRecord
                     */

                    double[] currentCentroidDouble = convertToDoubleArray(currentCentroid.getCentroidLongBits());

                    Record oldRecord = currentCluster.getCatalogPoint(idCatalog);

                    double oldDistance = distanceAlgorithm.calculate(
                            oldRecord.getData(), currentCluster.getCentroid());

                    if (currentDistance < oldDistance) {
                        currentCluster.removeRecord(oldRecord);

                        currentCluster.addRecord(record);
                        currentCluster.calculateCentroid();
                        if (centroidsTree.contains(currentCluster.getCentroid())){
                            System.out.println("o centroide novo ja existe 3");

                            currentCluster.removeRecord(record);
                            currentCluster.addRecord(oldRecord);
                            currentCluster.calculateCentroid();


                            centroidsCandidatesDistance.remove(currentCluster);
                            centroidsTree = searchCentroid2(record, centroidsCandidatesDistance,
                                    idCatalog,centroidsTree);


                        }else{


                            tamanhoantes = centroidsTree.size();
                            centroidsTree.remove(currentCentroidDouble);


                            centroidsTree
                                    .put(currentCluster.getCentroid(), currentCluster);

                            tamanhodepois = centroidsTree.size();
                            if (tamanhoantes != tamanhodepois) {
                                System.out.println(" Centroide " + Arrays.toString(currentCluster.getCentroid()) + " existe novo? " + centroidsTree.contains(currentCluster.getCentroid())+ " e o velho? " + Arrays.toString(currentCentroidDouble) + " " + centroidsTree.contains(currentCentroidDouble));
                                System.out.println(currentCluster.getRecords().get(0).getId() );
                            }

                            centroidsCandidatesDistance.clear();

					/* search another cluster for oldRecord */
                            Iterator<PhTreeVD.PVDEntry<Cluster>> results = rangeQuery(
                                    centroidsTree, oldRecord.getData(), 0.003);




                            for (; results
                                    .hasNext();) {

                                Cluster otherCluster = results.next().getValue();



                                double distance = distanceAlgorithm.calculate(
                                        oldRecord.getData(), otherCluster.getCentroid());

                                if (!currentCluster.equals(otherCluster)) {

                                    centroidsCandidatesDistance.put(otherCluster,
                                            distance);

                                }

                            }
                            centroidsTree=  searchCentroid2(oldRecord, centroidsCandidatesDistance,
                                    idCatalog, centroidsTree);
                        }
                    }else {

                        centroidsCandidatesDistance.remove(currentCentroid);
                        centroidsTree = searchCentroid2(record, centroidsCandidatesDistance, idCatalog, centroidsTree);
                    }
                }

                break;
        }

        return centroidsTree;
    }



    /**
     * Read a single records and store it in records
     */
    private Record readRecord(String line) throws NumberFormatException {




        String[] strRecord = line.split(separator);
        double[] record = new double[attributesLenght];
        for (int i = 0; i < attributesUsed.length; i++) {
            record[i] = Double.parseDouble(strRecord[attributesUsed[i]]
                    .trim());
        }
        Record r = new Record(record);

            r.setId(Integer.parseInt(strRecord[1]));
            r.setIdCatalog(Integer.parseInt(strRecord[4]));
            r.setClusterReal(Integer.parseInt(strRecord[strRecord.length - 1]));
            r.setPartitionName(strRecord[0]);
            /** Verifica se é da fronteira */

            for (int i = 0; i < partitionsListBroadcast.value().size(); i++) {

                raFronteira = Double.valueOf(partitionsListBroadcast.value().get(i));
                if (r.getRa()*(r.getRa() - 2*raFronteira) <= epsilon*epsilon - raFronteira*raFronteira) {
                    r.setFronteira(true);
                    r.setPartitionName(partitionsMapBroadCast.value().get(Double.valueOf(partitionsListBroadcast.value().get(i))));
                    //System.out.println(r.getDataString() +" eh da Fronteira " + partitionsMap.get(Double.valueOf(partitions.get(i))));
                }
            }
        //Casos 0 e 360:
        raFronteira = 0.0;
        if (r.getRa()*(r.getRa() - 2*raFronteira) <= epsilon*epsilon - raFronteira*raFronteira) {
            r.setFronteira(true);
            r.setPartitionName(partitionsMapBroadCast.value().get(raFronteira));
            //System.out.println(r.getDataString() +" eh da Fronteira " + partitionsMap.get(Double.valueOf(partitions.get(i))));
        }

        raFronteira = 360.0;
        if (r.getRa()*(r.getRa() - 2*raFronteira) <= epsilon*epsilon - raFronteira*raFronteira) {
            r.setFronteira(true);
            r.setPartitionName(partitionsMapBroadCast.value().get(raFronteira));
            //System.out.println(r.getDataString() +" eh da Fronteira " + partitionsMap.get(Double.valueOf(partitions.get(i))));
        }




        strRecord = null;

        line = null;
        return r;

    }

    /**
     * Range Query, testei apenas para duas dimens?es
     *
     * @param point
     * @param epsilon
     * @return PhTreeVD.PVDIterator<Cluster>
     */
    public PhTreeVD.PVDIterator<Cluster> rangeQuery(PhTreeVD<Cluster> centroidsTree2,
                                                           double[] point, double epsilon) {
        double[] min = new double[point.length];
        double[] max = new double[point.length];

        for (int i = 0; i < point.length; i++) {
            min[i] = (point[i] - epsilon);
            max[i] = (point[i] + epsilon);
        }

        return centroidsTree2.query(min, max);
    }

    public static double[] convertToDoubleArray(long[] longArray) {
        double[] doubleArray = new double[attributesLenght];
        for (int i = 0; i < attributesLenght; i++) {
            doubleArray[i] = BitTools.toDouble(longArray[i]);
        }
        return doubleArray;

    }

    /*private static void computeFmeasure2(int numberOfCatalogs) {

        Collection<Cluster> clusters = new ArrayList<>();
        PVDIterator<Cluster> clustersPHtree = centroidsTree.queryExtent();
        int recordsSize = 0;

        while (clustersPHtree.hasNext()) {
            recordsSize = 0;
            Cluster cluster = clustersPHtree.nextValue();
            recordsSize += cluster.getRecords().size();
            clusters.add(cluster);
            System.out.println("cluster " + cluster.getCentroidString() + " record size" + recordsSize);


        }

        ArrayList<Record> recordsCluster = new ArrayList<Record>();
        Integer clustersCorretos = 0;
        int clustersErrados = 0;
        int clusterId1 = 0, clusterId2 = 0, clusterId3 = 0;

        if (numberOfCatalogs == 2) {
            for (Cluster cluster : clusters) {

                recordsCluster = cluster.getRecords();

                if (recordsCluster.size() == 2) {
                    clusterId1 = recordsCluster.get(0).getId();
                    clusterId2 = recordsCluster.get(1).getId();
                    if ((clusterId1 == clusterId2)) {
                        clustersCorretos++;
                    } else {
                        clustersErrados++;
                    }
                } else {
                    clustersErrados++;
                }
            }

        }

        if (numberOfCatalogs == 3) {
            for (Cluster cluster : clusters) {
                recordsCluster = cluster.getRecords();
                if (recordsCluster.size() == 3) {
                    clusterId1 = recordsCluster.get(0).getId();
                    clusterId2 = recordsCluster.get(1).getId();
                    clusterId3 = recordsCluster.get(2).getId();
                    if ((clusterId1 == clusterId2)
                            && (clusterId2 == clusterId3)) {
                        clustersCorretos++;
                    } else {
                        clustersErrados++;
                    }
                } else {
                    clustersErrados++;
                }
            }

        }

        StringBuilder imprimir = new StringBuilder(680);
        double clustersCorretosDouble = clustersCorretos;
        double numeroOriginalCatalog = recordsSize / numberOfCatalogs;
        double precision = clustersCorretosDouble / clusters.size();
        double recall = clustersCorretosDouble / numeroOriginalCatalog;
        double fmeasure = (2 * precision * recall) / (precision + recall);

        // "Obs: Essa classifica??o de quantidade de clusters s? funciona para os cat?logos chacoalhados gerados pela classe catalogShake.java e utilizando no m?ximo 6 cat?logos. A porcentagem ? qtdClustersCorretos/qtdClustersOriginais, considerando o catalogo original como id = 1");

        imprimir.append("\n" + clustersCorretos + " clusters corretos\n"
                + clustersErrados + " clusters errados\n" + "Precision "
                + precision + "\nRecall " + recall + "\n" + "F-measure "
                + fmeasure + "\n" +
                + clustersCorretos + " " + centroidsTree.size() + " "
                + clustersCorretosDouble / numeroOriginalCatalog * 100 + "% "
                /* + "s" + "\n centroidsIguais = "+ qtdCentroideNovo*///);

    //System.out.println(imprimir);

    //}



    private void computeFmeasure3(List<Cluster> clusters) {
        System.out.println("clusters.size() = " + clusters.size());
        ArrayList<Record> recordsCluster;
        Integer clustersCorretos = 0;
        int clustersErrados = 0;

        int sizeCluster;

        for (Cluster cluster : clusters) {
            recordsCluster = cluster.getRecords();

            sizeCluster = recordsCluster.size();
            System.out.println("sizeCluster = " + sizeCluster);
            //todo Existe cluster de tamanho 0, verificar porque isso acontece.
            if(sizeCluster > 0) {
                boolean certo = true;
                // int id1 = recordsCluster.get(0).getId();
                Record record1 = recordsCluster.get(0);

                if (sizeCluster == 1 && record1.getQtdClusterReal() == 1) {
                    clustersCorretos++;
                } else {
                    for (int i = 1; i < sizeCluster; i++) {
                        if (record1.getId() != recordsCluster.get(i).getId()) certo = false;
                    }
                    if (certo) {
                        if (sizeCluster == record1.getQtdClusterReal())
                            clustersCorretos++;
                        else {
                            clustersErrados++;
                        }
                    } else clustersErrados++;
                }

            }

        }





        StringBuilder imprimir = new StringBuilder(680);
        double clustersCorretosDouble = clustersCorretos;

        System.out.println("primeiroRecordId = " + primeiroRecordId);
        System.out.println("ultimoRecordId = " + ultimoRecordId);
        double precision = clustersCorretosDouble / clusters.size();
        double recall = clustersCorretosDouble /(ultimoRecordId - primeiroRecordId + 1) ;

        System.out.println(" clustersCorretosDouble = " + clustersCorretosDouble);
        double fmeasure = (2 * precision * recall) / (precision + recall);

        // "Obs: Essa classifica??o de quantidade de clusters s? funciona para os cat?logos chacoalhados gerados pela classe catalogShake.java e utilizando no m?ximo 6 cat?logos. A porcentagem ? qtdClustersCorretos/qtdClustersOriginais, considerando o catalogo original como id = 1");

        imprimir.append("\n").append(clustersCorretos).append(" clusters corretos\n").append(clustersErrados).append(" clusters errados\n").append("Precision ").append(precision).append("\nRecall ").append(recall).append("\n").append("F-measure ").append(fmeasure).append("\n").append(+clustersCorretos).append(" ").append(clusters.size()).append(" ").append(clustersCorretosDouble / 3 * 100).append("% ");

        System.out.println(imprimir);

        double grau = grauAmbiguidadeSoma;


        sizeClusters = sizeClusters -1.0;


        double maxAmb = sizeClusters*sizeRecords;


        //System.out.println(quantidadeSearchCent + " vezes no search Centroide");
        System.out.println("Grau de Ambiguidade " + grauAmbiguidadeSoma);
        System.out.printf("Grau M�dio de Ambiguidade = %.8f\n", grauAmbiguidadeSoma / sizeRecords);
        System.out.printf("M�trica de Ambiguidade = %.8f\n", grauAmbiguidadeSoma/maxAmb);


    }






    private Iterable<Cluster> naclusterReduce(Iterator<List<Cluster>> lines) {


        Collection<Record> records = new ArrayList<>();
        PhTreeVD<Cluster> phtree = new PhTreeVD<>(attributesLenght);




        while (lines.hasNext()) {

            List<Cluster> clusterLista = lines.next();
            for (Cluster cluster : clusterLista) {
                records.addAll(cluster.getRecords());

                cluster.resetRecords();

                phtree.put(cluster.getCentroid(), cluster);
            }




        }


        /** Aqui fica a itera��o do algoritmo, precisa ser automatizada */
       // for (int i = 0; i < 2; i++) {

            phtree = nacluster2(phtree, records);

        //}



        PhTreeVD.PVDIterator<Cluster> iter = phtree.queryExtent();

        List<Cluster> clusters = new ArrayList<>();


        for (; iter.hasNext();) {
            clusters.add(iter.next().getValue());

        }

        /***
         * Aqui ja podemos fazer o lance da fronteira
         *
         * Para cada objeto da fronteira, procurar pelos centr�ides candidatos. Para cada centroide
         * encontrado, pegar seus objetos e procurar para cada um deles (dos objetos) os centroides
         * candidatos e assim sucessivamente.
         * Assim eu vou encontrando o caminho
         */



        return clusters;



    }


    public void setThreshold(Threshold threshold) {
        this.threshold = threshold;
    }

    private PhTreeVD<Cluster> resetRecordsFromClusters(PhTreeVD<Cluster> centroidsTree) {

        PhTreeVD.PVDIterator<Cluster> clusters = centroidsTree.queryExtent();

        Cluster cluster;
        for (Iterator<PhTreeVD.PVDEntry<Cluster>> iterator = clusters; iterator.hasNext();) {
            cluster = iterator.next().getValue();
            cluster.resetRecords();
            //  System.out.println("Resetando clusters " + cluster.getRecords().size());
        }


        return centroidsTree;
    }

}


