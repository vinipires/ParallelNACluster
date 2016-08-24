import java.io.*;


public class Application {

    /** File name to be used */
    private String fileName = "catalogsShaked.txt";
    /** Output path to be used */
    private String outputPath = "~/saidaSpark";

    /** File name to be used */
    private String partitionFileName = "partitionFile.txt";

    /** file to save the incomplete data */
    @SuppressWarnings("unused")
    private String incompleteDataFile = "incomplete.data";

    /** Catalog Number to recount k value. This is the last column from data. */
    @SuppressWarnings("unused")
    private int catalogNumber = 2;

    /** Method used to check the stop condition */
    private int stopMethod = NAClusterSpark.STOP_SAME_CENTROIDS;
    /** if the ConstantEpochs method is used number of epochs */
    private int stopEpochs = 1;





    /** index of Attributes to be used */
    private static int[] attributesUsed = { 1, 2 };

    private static int x = 0;





    public static void main(String[] args) throws Exception {





        if (args.length < 3) {
            System.out
                    .println("Usage: java -XX:+UseConcMarkSweepGC -jar NACluster.jar inputPath outputPath 0 1 \nargs[0] = InputPath \nargs[1] = OutputPath \nargs[2] = partitionFile.txt \n" +
                            "args[3] = Threshold (1) (0 Constant Epochs, 1 Same Centroids)\nif args[3]==0 --> args[4] = Epochs (10) ");
            System.out
                    .println("Each line in Filename.txt in inputPath has: idRecord(object),ra,dec,idCatalog");
            System.out
                    .println("Example: \n1,315.024512,-35.236652,1\n2,315.058928,-35.286324,2\n3,315.090892,-35.277237,3\n4,315.001386,-35.20591,1\n5,315.008551,-35.208263,2");

        } else {
            Application app = new Application();

            try {
                app.readArgs(args);
            } catch (IOException e) {
                System.out
                        .println("Error occurred while reading input data.....");
            }


            app.run();
        }




    }


    private void readArgs(String[] args) throws IOException {

        String input;

        /**
         * "File Name (catalogsShaked.txt):");
         */
        input = args[0];
        StringBuilder filename = new StringBuilder("Input Path: ");
        if (input == null || input.trim().equals("")
                || new File(input).exists()) {
            System.out.println(filename.append(input));
        } else {
            System.out.println("File doesn't exists");
        }

        if (input != null && !input.trim().equals("")) {
            fileName = input;
        }

        /**
         * "Output Path");
         */
        input = args[1];

        StringBuilder output = new StringBuilder("Output Path: ");
        if (input == null || input.trim().equals("")
                || new File(input).mkdir()) {
            System.out.println(output.append(input));
        } else {
            System.out.println("Output path já existe");
        }

        if (input != null && !input.trim().equals("")) {
            outputPath = input;
        }


        /**
         * "File Name (partitionFile.txt):");
         */
        input = args[2];
        StringBuilder fileName3 = new StringBuilder("Partition File Name: ");
        if (input == null || input.trim().equals("")
                || new File(input).exists()) {
            System.out.println(fileName3.append(input));
        } else {
            System.out.println("File doesn't exists");
        }

        if (input != null && !input.trim().equals("")) {
            partitionFileName = input;
        }


        /**
         * "Threshold (1) (0 Constant Epochs, 1 Same Centroids):");
         *//*
        input = args[3];
        if (input == null || input.trim().equals("")) {
            System.out
                    .println("Threshold (1) (0 Constant Epochs, 1 Same Centroids)");
        } else {
            try {
                int i = Integer.parseInt(input);
                if (i == 0 || i == 1) {
                    stopMethod = i;
                    System.out.println("Threshold (" + i
                            + ") (0 Constant Epochs, 1 Same Centroids)");
                } else {
                    System.out.println("Invalid number, 0 or 1 expected");
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid number");
            }
        }

        if (stopMethod == 0) {


            if (args.length == 4) {
                System.out.println("Epochs (10)");
            }
            if (args.length == 5) {

                try {
                    stopEpochs = Integer.parseInt(args[4]);
                    System.out.println("Epochs (" + stopEpochs + ")");
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number");
                }

            }
        }*/

    }

    private void run() throws ClassNotFoundException, IOException {


        System.setProperty("hadoop.home.dir", "c:\\winutils\\");



        /**
         *
         * Create a Java Spark Context.
         *
         * Para serializar uma classe acrescentar ao SparkConf: .registerKryoClasses(new Class<?>[]{
         * Class.forName("Data"),
         * Class.forName("Cluster")})
         *
         **/



        NAClusterSpark clustering = new NAClusterSpark(fileName, outputPath, partitionFileName);

        // set the stopping criteria
        if (stopMethod == NAClusterSpark.STOP_SAME_CENTROIDS) {
            clustering.setThreshold(new SameCentroidThreshold());
        } else {
            clustering.setThreshold(new ConstantEpochsThreshold(stopEpochs));
        }

        clustering.run();
    }



}