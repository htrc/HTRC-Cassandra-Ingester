package edu.indiana.d2i.ingest;

import java.util.List;

/*
 * Class to update one or more columns of the volume text column family in Cassandra.
 */
public abstract class Updater {

	// update the value(s) corresponding to a single volume id
	// result: true if the update succeeded, false otherwise
	public abstract boolean update(String volumeId);

	// update the value(s) corresponding to volume ids in a list
	// resut: the list of volume ids for which the update failed
	public abstract List<String> update(List<String> volumeIds);
}
