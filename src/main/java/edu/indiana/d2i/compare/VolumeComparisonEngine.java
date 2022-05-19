package edu.indiana.d2i.compare;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

import edu.indiana.d2i.ingest.cassandra.CassandraManager;
import edu.indiana.d2i.ingest.util.Tools;

// VolumeComparisonEngine
// for a given set of volumes, this class compares, for each volume, the page sequences of the 
// volume if it exists in Cassandra, with the page sequences in the zip file of the volume 
// in the pairtree, e.g., if the volume in Cassandra has more pages a new zip file for the 
// volume rsynced into the pairtree
// example use:
// java -cp htrc-cassandra-ingester-jar-with-dependencies.jar \
//   edu.indiana.d2i.compare.VolumeComparisonEngine ids-to-compare.txt
public class VolumeComparisonEngine {
	public static void main(String[] args) throws FileNotFoundException {
		// args[0] is the path of the file containing volume ids for which comparsion is performed
		String idListFilePath = args[0];
		List<String> volumeIdsToCompare = Tools.getVolumeIds(new File(idListFilePath));
		VolumeComparison vc = new VolumeComparison();
		for (String volumeId: volumeIdsToCompare) {
			vc.compareNewZipWithExistingVolume(volumeId);
		}
		double timeForSelectPageSequences = (double)vc.getTotalTimeForSelects()/volumeIdsToCompare.size();
		double timeForSelectPageSeqInSeconds = timeForSelectPageSequences/1_000_000_000.0;
		System.out.println("Time taken by SELECT of page sequences of a volume = " + timeForSelectPageSeqInSeconds + " seconds");
		vc.close();
		CassandraManager.shutdown();
	}
}
