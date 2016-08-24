

import ch.ethz.globis.pht.PhTreeVD;

import java.util.Collection;

/**
 * Implements a constant iteration NACluster. After a constant number of
 * iteration isDone return true.
 */
public class ConstantEpochsThreshold implements Threshold {
	/** Number of iterations */
	private final int itr;
	/** current iteration number */
	private int curItr = 0;

	/**
	 * 
	 * @param itr
	 */
	public ConstantEpochsThreshold(int itr) {
		this.itr = itr;
	}

	public int getCurrentIteration() {
		return curItr;
	}

	/**
	 * Returns true if the iteration counter is greater than or equal to the
	 * configure value.
	 * 
	 * @param phtree
	 *            set of partitions
	 * @return true if the no of iterations > iteration count
	 */
	public boolean isDone(PhTreeVD<Cluster> phtree) {
		curItr++;
		System.out.println("itr = " + itr);
		System.out.println("curItr = " + curItr);
		System.out.println("(curItr > itr) = " + (curItr > itr));
		return curItr > itr;
	}
}
