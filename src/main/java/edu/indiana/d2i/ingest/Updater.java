package edu.indiana.d2i.ingest;

import java.util.List;

public abstract class Updater {

	public abstract boolean update(List<String> volumeIds);
}
