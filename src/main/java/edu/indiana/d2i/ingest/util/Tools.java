package edu.indiana.d2i.ingest.util;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.util.METSParser.VolumeRecord;
import gov.loc.repository.pairtree.Pairtree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Tools {
	private static Logger log = LogManager.getLogger();
	public static List<String> getVolumeIds(File inputFile) {
	//	File inputFile = new File(Configuration.getProperty("VOLUME_ID_LIST"));
		List<String> list = new LinkedList<String>();
		
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), "UTF-8"));
			String id = null;
			while((id = br.readLine()) != null) {
				list.add(id);
			}
			br.close();
		} catch (UnsupportedEncodingException e) {
			log.error("unsupported encoding", e);
		} catch (FileNotFoundException e) {
			log.error("volume id list file not found", e);
		} catch (IOException e) {
			log.error("volume id list read failed", e);
		} 
		
		return list;
	}
	
	/**
	 * get the path of the volume with given volume ID under pairtree
	 * e.g. "loc/pairtree_root/ar/k+/=1/39/60/=t/9w/09/kd/5k/ark+=13960=t9w09kd5k"
	 * @param volID		the volume ID that this method get path for
	 * @return			the relative path of this given volume ID under pairtree
	 */
	public static String getPairtreePath(String volID) {
		Pairtree pt = new Pairtree();
		StringBuilder basePathBuilder = new StringBuilder();
		
		String[] parts = volID.split("\\.", 2); // parts[0] is the abbreviation of schools, part[1] is the id part
		String cleanIdPart = pt.cleanId(parts[1]);
		
		basePathBuilder.append(parts[0]).append('/').append("pairtree_root");
		return pt.mapToPPath(basePathBuilder.toString(), parts[1], cleanIdPart);
	}
	
	/**
	 * parse mets file for volume with volID using METSParser
	 * @param volID				the volume ID
	 * @param metsFile			the corresponding metsFile
	 * @return					VolumeRecord instance that contains all the parsed information from METS xml file
	 */
	public static VolumeRecord getVolumeRecord(String volID, File metsFile) {
		
		 XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
	        
		 VolumeRecord volumeRecord = new VolumeRecord(volID);
	        
	        // copyright is assumed to be public domain for all volumes 
	        volumeRecord.setCopyright(CopyrightEnum.PUBLIC_DOMAIN); 
	        
	        METSParser metsParser = new METSParser(metsFile, volumeRecord, xmlInputFactory);
	        try {
				metsParser.parse();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (XMLStreamException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
	        // that's it. it is now parsed and the volumeRecord should be populated by the parser
	        return volumeRecord;
	}

	/**
	 * return the file with specified relative path and file name in pairtrees
	 * @param leafPath				the relative pairtree path that this file is under
	 * @param zipFileName			the file name
	 * @return						the File instance with specified relativePairtreePath and fileName. Null if it does not exist.
	 */
	public static File getFileFromPairtree(String relativePairtreePath, String fileName) {
		// for each vm-named directory, see if this file is under it
		for(HtrcVM vm : HtrcVM.values()){
			StringBuilder absolutePathBuilder = new StringBuilder();
			absolutePathBuilder.append(Constants.ROOT_PATH).append(Constants.SEPERATOR)
			.append(vm).append(Constants.SEPERATOR).append(Constants.TO_PAIRTREE_PATH)
			.append(Constants.SEPERATOR).append(relativePairtreePath).append(Constants.SEPERATOR)
			.append(fileName); 
			
			File file = new File(absolutePathBuilder.toString());
			if(file.exists()){ // if exist, return this file instantly
				return file;
			}
			//System.out.println(absolutePathBuilder.toString());
		}
		return null;
	}

	  /**
     * Method to compute checksum using the specified checksum algorithm
     * @param contents contents whose checksum is to be computed
     * @param algorithm name of the checksum computation algorithm
     * @return String representation of the computed checksum in hexadecimal format
     * @throws NoSuchAlgorithmException thrown if the specified algorithm is not known
     */
    public static String calculateChecksum(byte[] contents, String algorithm) throws NoSuchAlgorithmException {
        StringBuilder checksumBuilder = new StringBuilder();
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        digest.update(contents);
        byte[] checksumBytes = digest.digest();
        for (byte bite : checksumBytes) {
            checksumBuilder.append(byteToHex(bite));
        }
        return checksumBuilder.toString();
    }
    
    static final char[] hexDigit = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
	private static final int SEQUENCE_LENGTH = 8;
    /**
     * Method to convert a byte to its 2-digit zero-padded hexadecimal representation
     * @param bite a byte
     * @return a 2-digit zero-padded hexdecimal representation of the byte
     */
    public static char[] byteToHex(byte bite) {
        char[] hexByteChars = new char[2];
        hexByteChars[0] = hexDigit[(int)(bite & 0xFF) / 16];
        hexByteChars[1] = hexDigit[(int)(bite & 0xFF) % 16];
        return hexByteChars;
    }

	public static String cleanId(String id) {
		Pairtree pt = new Pairtree();
		String[] parts = id.split("\\.", 2);
		String cleanIdPart = pt.cleanId(parts[1]);
		return new StringBuilder(parts[0]).append(".").append(cleanIdPart)
				.toString();
	}

	/**
	 * pick <code>size</code> number of volume IDs from file <code>allVolumes</code> and write them to VOLUME_ID_LIST from configuration
	 * @param allVolumes
	 * @param size
	 */
	public static void generateRandomInputVolumeList(File allVolumes, int size) {
		List<String> list = getVolumeIds(allVolumes);
		Collections.shuffle(list);
		List<String> subList = list.subList(0, size);
		File inputFile = new File(Configuration.getProperty("VOLUME_ID_LIST"));
		try {
			PrintWriter pw = new PrintWriter(inputFile);
			for(String volumeId : subList) {
				pw.println(volumeId);
				pw.flush();
			}
			pw.flush();pw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static List<String> generateSequences(int num) {
		int max = 250;
		List<String> sequences = new LinkedList<String>();
		Random random = new Random();
		for(int x=0; x<num; x++) {
			int seq = random.nextInt(max);
			String orderString = Integer.toString(seq);
	        
	        StringBuilder sequenceBuilder = new StringBuilder();
	        
	        int digitCount = orderString.length();
	        for (int i = digitCount; i < SEQUENCE_LENGTH; i++) {
	            sequenceBuilder.append('0');
	        }
	        sequenceBuilder.append(orderString);
	        sequences.add(sequenceBuilder.toString());
		}
		return sequences;
	}
	
	public static void main(String[] args) {
		String volumeId = "loc.ark:/13960/t7kp9549h";
		
		String pairtreePath = Tools.getPairtreePath(volumeId);
		System.out.println(pairtreePath);
	}

	/**
	 * to be implemented
	 * @param volumeId
	 * @return
	 */
	public static String getMarcString(String volumeId) {
		// TODO Auto-generated method stub
		return "";
	}
}
