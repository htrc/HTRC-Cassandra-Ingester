package edu.indiana.d2i.ingest.cassandra;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.util.Configuration;

public class MarcProcessor {
	private static Logger logger = LogManager.getLogger(MarcProcessor.class);
	
	// the volume id is in 974$u of the MARC record; the expected json is shown below:
	// {
	// 	  "leader":"...",
	//	  "fields":[
	//	    {"00m":"..."},
	//	    ...
	//	    {"nnn":{"subfields":[{"a":"..."}, {"b":"..."}, ...]
	//	            "ind1":"...",
	//		        "ind2" : "..."}},
	//	    ...
	//	  ]
	//	}
	private static final String FIELDS_KEY = "fields";
	private static final String KEY_IN_FIELDS_WITH_VOLID = "974";
	private static final String SUBFIELDS_KEY = "subfields";
	private static final String KEY_IN_SUBFIELDS_WITH_VOLID = "u";

	private static final String NO_VOLID_FOUND_STR = "no-volid-found";
	
	private String marcJsonFilesFolder;
	private String[] marcJsonFiles;
	private MarcIngester marcIngester;
	private String outputFile;

	public MarcProcessor() {
		super();
		this.marcJsonFilesFolder = Configuration.getProperty(Constants.PK_MARC_JSON_FILES_FOLDER, Constants.DEFAULT_MARC_JSON_FILES_FOLDER);
		this.marcJsonFiles = this.parseArrayString(Configuration.getProperty(Constants.PK_MARC_JSON_FILES, Constants.DEFAULT_MARC_JSON_FILES));
		this.outputFile = Configuration.getProperty(Constants.PK_MARC_INGESTER_OUTFILE, Constants.DEFAULT_MARC_INGESTER_OUTFILE);
		this.marcIngester = new MarcIngester();
	}
		
	// processes (ingests) all marc records in all files in this.marcJsonFiles
	public void processAll() {
		long start = System.currentTimeMillis();
		logger.info("MARC_INGESTER: starting ingestion of all MARC records in files {}", arrayAsString(this.marcJsonFiles));
		List<String> uningestedVolIds = this.processLoop(volId -> true, volId -> {}, () -> false);
		logger.info("MARC_INGESTER: No. of volumes for which ingest was attempted but unsuccessful = {}", uningestedVolIds.size());
		long end = System.currentTimeMillis();	
		logger.info("MARC_INGESTER: Time taken = {} seconds", (end - start)/1000.0);
		this.writeResultVolIdsToFile(uningestedVolIds, this.outputFile);
	}

	// processes (ingests) marc records for volume ids in the given file, finding the corresponding marc records in this.marcJsonFiles
	public void process(String volIdsFile) {
		try (Stream<String> volIdsStream = Files.lines(Paths.get(volIdsFile))) {
			this.process(volIdsStream.map(volId -> volId.trim()).collect(Collectors.toSet()));
		} catch (IOException e) {
			logger.error("MARC_INGESTER: Exception while reading file {}", volIdsFile, e);
		}
	}
	
	// processes (ingeste) marc records for volume ids in the given list, finding the corresponding marc records in this.marcJsonFiles
	public void process(List<String> volIdsList) {
		if (volIdsList != null) {
			process(new HashSet<String>(volIdsList));
		}
	}
	
	// processes (ingeste) marc records for volume ids in the given set, finding the corresponding marc records in this.marcJsonFiles
	// warning: volIdsSet is modified by this method
	public void process(Set<String> volIdsSet) {
		long numVolIdsToProcess = volIdsSet.size();
		long start = System.currentTimeMillis();	
		logger.info("MARC_INGESTER: No. of volumes ids for which MARC records need to be ingested = {}", volIdsSet.size());
		List<String> uningestedVolIds = processLoop(volId -> volIdsSet.contains(volId), volId -> volIdsSet.remove(volId), () -> volIdsSet.isEmpty());		
		logger.info("MARC_INGESTER: Ingested MARC records for {} volumes", numVolIdsToProcess - (uningestedVolIds.size() + volIdsSet.size()));
		long end = System.currentTimeMillis();	
		logger.info("MARC_INGESTER: Time taken = {} seconds", (end - start)/1000.0);
		
		// uningestedVolIds contains the list of volume ids on which ingest was attempted but unsuccessful; volIdsSet is modified in processLoop,
		// and now contains the list of volume ids for which MARC records were not found (and therefore, no ingest was attempted for these volume 
		// ids)
		this.writeResultVolIdsToFile(uningestedVolIds, volIdsSet.toArray(new String[0]), this.outputFile);
	}

	// reads and processes marc records in the configured marc json files; returns the list of volume ids for which ingest was attempted but not 
	// successful
	private List<String> processLoop(Predicate<String> volIdTester, Consumer<String> updateOnTestSucc, BooleanSupplier completionTester) {
		Stream<String> resultVolIds = Stream.empty();
		if (this.marcIngester.marcColFamilyExists()) {	
			for (String marcJsonFile: this.marcJsonFiles) {
				List<String> uningestedVolIds = processFile(this.marcJsonFilesFolder + File.separator + marcJsonFile, volIdTester, updateOnTestSucc, completionTester);
				resultVolIds = Stream.concat(resultVolIds, uningestedVolIds.stream());
			}
		} else {
			logger.error("MARC_INGESTER: No column family named {}; ingestion cannot proceed", this.marcIngester.getMarcColFamily());
		}
		return resultVolIds.collect(Collectors.toList());
	}

	// read and process MARC records from file specified by marcJsonFilePath; returns the list of volume ids for which ingest was attempted but not 
	// successful
	// 
	// volIdTester - contains the test method to be called on the volume id extracted from the marc record; the "processing" (ingest) occurs only if
	//               the test is positive for the volume id; set to (volId-> true) if all MARC records in the file are to be processed
	//
	// updateOnTestSucc - contains the method that should be called on a volume id, if the test in volIdTester succeeds on the volume id; set to 
	//                    (volId -> {}) for no-op
	//
	// processCompletionTester - contains a method that is called to check if processing can be terminated; the method in processCompletionTester
	//                           takes no arguments, and returns true if processing can be terminated, false otherwise; set to (() -> false) if all 
	//                           MARC records in the specified file are to be processed
	private List<String> processFile(String marcJsonFilePath, Predicate<String> volIdTester, Consumer<String> updateOnTestSucc, 
			BooleanSupplier processCompletionTester) {
		ArrayList<String> uningestedVolIds = new ArrayList<String>();
		JSONParser parser = new JSONParser();
		try (BufferedReader br = new BufferedReader(new FileReader(marcJsonFilePath))) {
			String line;
			int count = 0;
			while ((line = br.readLine()) != null && !(processCompletionTester.getAsBoolean())) {
				try {
					JSONObject volMarc = (JSONObject) parser.parse(line);
					String volumeid = this.getVolumeIdFromMarc(volMarc);
					if (volumeid.equals(NO_VOLID_FOUND_STR)) {
						logger.error("MARC_INGESTER: No volume id found at line {}", count + 1);
					} else if (volIdTester.test(volumeid)) {
						updateOnTestSucc.accept(volumeid);
						// logger.debug("MARC_INGESTER: Volume id {} found in file {}", volumeid, this.getFileName(marcJsonFilePath));
						Boolean ingestRes = this.marcIngester.ingestVolumeMarc(volumeid, line);
						if (! ingestRes) {
							uningestedVolIds.add(volumeid);
						}
					}
				} catch (ParseException e) {
					logger.error("MARC_INGESTER: Exception while parsing line {} in marc json file", count + 1, e);
				} catch (Exception e) {
					logger.error("MARC_INGESTER: Exception while reading json at line {}", count + 1, e);
				}
				count++;
			}
			logger.info("MARC_INGESTER: Processed {} lines in file {}", count, marcJsonFilePath);
		} catch (IOException e) {
			logger.error("MARC_INGESTER: Exception while reading marc json file {}", marcJsonFilePath, e);
		}
		return uningestedVolIds;
	}

	private void writeResultVolIdsToFile(List<String> uningestedVolIds, String outputFile) {
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
			bw.write(String.format("Volume ids for which ingest was attempted but unsuccessful:%n"));
			for (String volId: uningestedVolIds) {
				bw.write(String.format("%s%n", volId));
			}
		} catch (IOException e) {
			logger.error("MARC_INGESTER: Exception while writing to output file {}", outputFile, e);
		}
	}
	
	private void writeResultVolIdsToFile(List<String> uningestedVolIds, String[] volIdsNotInJsonFiles, String outputFile) {
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
			bw.write(String.format("Volume ids for which ingest was attempted but unsuccessful:%n"));
			for (String volId: uningestedVolIds) {
				bw.write(String.format("%s%n", volId));
			}
			bw.write(String.format("%nVolume ids for which MARC records were not found:%n"));
			for (String volId: volIdsNotInJsonFiles) {
				bw.write(String.format("%s%n", volId));
			}
		} catch (IOException e) {
			logger.error("MARC_INGESTER: Exception while writing to output file {}", outputFile, e);
		}
	}
	
	// extracts the volume id from the MARC record for a volume; see above for expected json, and field of volume id 
	private String getVolumeIdFromMarc(JSONObject volMarc) {
		JSONArray fieldsArray = (JSONArray) volMarc.get(FIELDS_KEY); 
		if (fieldsArray == null) {
			logger.error("MARC_INGESTER: Unable to find \"{}\" element in MARC json: {}", FIELDS_KEY, volMarc);
			return NO_VOLID_FOUND_STR;
		}
		
		return this.getValuesForKeyFromArray(KEY_IN_FIELDS_WITH_VOLID, fieldsArray)
				.findFirst()
				.map(jsonObjWithVolId -> this.getVolumeIdFromObjInMarc((JSONObject) jsonObjWithVolId))
				.orElse(NO_VOLID_FOUND_STR);
	}
	
	private String getVolumeIdFromObjInMarc(JSONObject jsonObjWithVolId) {
		JSONArray subfieldsArray = (JSONArray) jsonObjWithVolId.get(SUBFIELDS_KEY);
		if (subfieldsArray == null) {
			logger.error("MARC_INGESTER: Unable to find \"{}\" element in MARC json: {}", SUBFIELDS_KEY, jsonObjWithVolId);
			return NO_VOLID_FOUND_STR;
		}
		return (String) this.getValuesForKeyFromArray(KEY_IN_SUBFIELDS_WITH_VOLID, subfieldsArray).findFirst().orElse(NO_VOLID_FOUND_STR);
	}

	// filters from an array of JSONObjects, those JSONObjects that contain a mapping for the given key, and returns all the values that are mapped 
	// to the key in different JSONObjects; in practice, only one such object is expected in the result for KEY_IN_FIELDS_WITH_VOLID and 
	// KEY_IN_SUBFIELDS_WITH_VOLID
	private Stream<Object> getValuesForKeyFromArray(String key, JSONArray fields) {
		return Stream.of(fields.toArray()).filter(obj -> ((JSONObject) obj).containsKey(key)).map(obj -> ((JSONObject) obj).get(key));
	}
	
	private String arrayAsString(String[] a) {
		return Stream.of(a).collect(Collectors.joining(",", "[", "]"));
	}
	
	private String[] parseArrayString(String str) {
		final String DELIMITER_IN_PROPERTIES_FILE = ",";

		if (str == null) {
			return new String[0];
		}
		return Stream.of(str.split(DELIMITER_IN_PROPERTIES_FILE)).map(elem -> elem.trim()).toArray(String[]::new);
	}
}
