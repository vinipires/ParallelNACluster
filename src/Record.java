import java.util.Arrays;

/**
 * A single data records read from the file.
 */
public class Record {
	/** Double array as data */
	private double[] data;
	/** The true catalog classifier */
	private int idCatalog = 0;
	private int id;
	private boolean fronteira = false;
	private int qtdClusterReal;
	private String partitionName;

	public Record(double[] data) {
		this.data = data;
	}

	public void setIdCatalog(int idCatalog) {
		this.idCatalog = idCatalog;
	}

	public int getIdCatalog() {
		return idCatalog;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getPartitionName() {
		return partitionName;
	}

	public void setPartitionName(String partitionName) {
		this.partitionName = partitionName;
	}

	public int getId() {
		return id;
	}

	public void setClusterReal(int qtdClusterReal) {
		this.qtdClusterReal = qtdClusterReal;
	}

	public int getQtdClusterReal() {
		return qtdClusterReal;
	}

	/**
	 * Get the data as a double array
	 *
	 * @return data as a double array
	 */
	public double[] getData() {
		return data;
	}

	public double getRa(){
		return data[0];
	}

	public String getDataString(){
		return partitionName +" " + id + ", " + Arrays.toString(data) + ", " + idCatalog + " " + qtdClusterReal;
	}


	public boolean isFronteira() {
		return fronteira;
	}

	public void setFronteira(boolean fronteira) {
		this.fronteira = fronteira;
	}
}
