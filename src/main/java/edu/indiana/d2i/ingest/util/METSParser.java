/*
#
# Copyright 2013 The Trustees of Indiana University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----------------------------------------------------------------
#
# Project: B669
# File:  METSParser.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.ingest.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.StringTokenizer;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.indiana.d2i.ingest.util.METSParser.VolumeRecord.PageRecord;



/**
 * @author Yiming Sun
 *
 */
public class METSParser {
    
    enum ParsePhaseEnum {
        OCR_FILEGRP,
        FILE,
        FLOCAT,
        STRUCTMAP,
        VOLUME_DIV,
        PAGE_DIV,
        OCR_FPTR;
    }
    
   /* public enum CopyrightEnum {
        PUBLIC_DOMAIN,
        IN_COPYRIGHT;
    }*/

    
    static class PageDivBean {
        private int order;
        private String label;
        private String orderLabel;
        
        PageDivBean() {
            this.order = 0;
            this.label = null;
            this.orderLabel = null;
        }
        void setOrder(int order) {
            this.order = order;
        }
        int getOrder() {
            return order;
        }
        void setLabel(String label) {
            this.label = label;
        }
        String getLabel() {
            return label;
        }
        void setOrderLabel(String orderLabel) {
            this.orderLabel = orderLabel;
        }
        String getOrderLabel() {
            return orderLabel;
        }
        
    }
    
    public static class VolumeRecord {
        public static class PageRecord {
            
            protected static final int INITIAL_PAGE_FEATURE_LIST_SIZE = 20;
            
            private String ocrFilename;
            private String sequence;
            private String checksum;
            private String checksumType;
            private String label;
            private String id;
            private List<String> features;
            private long byteCount;
            private int characterCount;
            private int order;
            
            
            public PageRecord() {
                this.sequence = null;
                this.ocrFilename = null;
                this.checksum = null;
                this.checksumType = null;
                this.id = null;
                this.byteCount = 0L;
                this.characterCount = 0;
                this.label = null;
                this.order = 0;
                this.features = null;
            }
            
            public void setSequence(String sequence) {
                this.sequence = sequence;
            }
            public String getSequence() {
                return sequence;
            }
            public void setOcrFilename(String ocrFilename) {
                this.ocrFilename = ocrFilename;
            }
            public String getOcrFilename() {
                return ocrFilename;
            }
            public void setChecksum(String checksum, String checksumType) {
                this.checksum = checksum;
                this.checksumType = checksumType;
            }
            public String getChecksum() {
                return checksum;
            }
            public String getChecksumType() {
                return checksumType;
            }
            public void setByteCount(long byteCount) {
                this.byteCount = byteCount;
            }
            public long getByteCount() {
                return byteCount;
            }
            public void setCharacterCount(int characterCount) {
                this.characterCount = characterCount;
            }
            public int getCharacterCount() {
                return characterCount;
            }
            public void setLabel(String label) {
                this.label = label;
            }
            public String getLabel() {
                return label;
            }
            void setID(String id) {
                this.id = id;
            }
            String getID() {
                return id;
            }
            public void setOrder(int order) {
                this.order = order;
            }
            public int getOrder() {
                return order;
            }
            public void addFeature(String feature) {
                if (features == null) {
                    features = new ArrayList<String>(INITIAL_PAGE_FEATURE_LIST_SIZE);
                }
                features.add(feature);
            }
            public List<String> getFeatures() {
                return features;
            }
        }
        
        static final int INITIAL_HASHMAP_CAPACITY = 300;
        private String volumeID;
        private int pageCount;
        private HashMap<String, PageRecord> hashMapByFilename;
        private HashMap<String, PageRecord> hashMapByID;
        private CopyrightEnum copyright;
        private String metsContents;
        
        public VolumeRecord(String volumeID) {
            this.volumeID = volumeID;
            this.pageCount = 0;
            this.hashMapByFilename = new HashMap<String, PageRecord>(INITIAL_HASHMAP_CAPACITY);
            this.hashMapByID = new HashMap<String, PageRecord>(INITIAL_HASHMAP_CAPACITY);
            this.copyright = CopyrightEnum.PUBLIC_DOMAIN;
            this.metsContents = null;
        }
        public String getVolumeID() {
            return volumeID;
        }
        public void setPageCount(int pageCount) {
            this.pageCount = pageCount;
        }
        public int getPageCount() {
            return pageCount;
        }
        public void addPageRecord(PageRecord pageRecord) {
            String ocrFilename = pageRecord.getOcrFilename();
            String id = pageRecord.getID();
            hashMapByFilename.put(ocrFilename, pageRecord);
            hashMapByID.put(id, pageRecord);
        }
        public Set<String> getPageFilenameSet() {
           return hashMapByFilename.keySet(); 
        }
        public Set<String> getPageIDSet() {
            return hashMapByID.keySet();
        }
        public PageRecord getPageRecordByFilename(String filename) {
            return hashMapByFilename.get(filename);
        }
        public PageRecord getPageRecordByID(String id) {
            return hashMapByID.get(id);
        }
        
        public void setCopyright(CopyrightEnum copyright) {
            this.copyright = copyright;
        }
        public CopyrightEnum getCopyright() {
            return copyright;
        }
        public void setMETSContents(String metsContents) {
            this.metsContents = metsContents;
        }
        public String getMETSContents() {
            return metsContents;
        }
        public void updatePageCount() {
            pageCount = hashMapByFilename.size();
            pageCount = hashMapByID.size() > pageCount ? hashMapByID.size() : pageCount;
        }
    }
    

    
    
    public static final String METS_NAMESPACE = "http://www.loc.gov/METS/";
    public static final String XLINK_NAMESPACE = "http://www.w3.org/1999/xlink";
    public static final String EMPTY_NAMESPACE = "";
    
    public static final QName QN_ELE_FILEGRP = new QName(METS_NAMESPACE, "fileGrp");
    public static final QName QN_ATTR_FILEGRP_USE = new QName(EMPTY_NAMESPACE, "USE");
    
    public static final QName QN_ELE_FILE = new QName(METS_NAMESPACE, "file");
    public static final QName QN_ATTR_FILE_SEQ = new QName(EMPTY_NAMESPACE, "SEQ");
    public static final QName QN_ATTR_FILE_ID = new QName(EMPTY_NAMESPACE, "ID");
    public static final QName QN_ATTR_FILE_SIZE = new QName(EMPTY_NAMESPACE, "SIZE");
    public static final QName QN_ATTR_FILE_CHECKSUM = new QName(EMPTY_NAMESPACE, "CHECKSUM");
    public static final QName QN_ATTR_FILE_CHECKSUMTYPE = new QName(EMPTY_NAMESPACE, "CHECKSUMTYPE");
    
    public static final QName QN_ELE_FLOCAT = new QName(METS_NAMESPACE, "FLocat");
    public static final QName QN_ATTR_FLOCAT_XLINKHREF = new QName(XLINK_NAMESPACE, "href");
    
    public static final QName QN_ELE_STRUCTMAP = new QName(METS_NAMESPACE, "structMap");
    public static final QName QN_ATTR_STRUCTMAP_TYPE = new QName(EMPTY_NAMESPACE, "TYPE");
    
    public static final QName QN_ELE_DIV = new QName(METS_NAMESPACE, "div");
    public static final QName QN_ATTR_DIV_TYPE = QN_ATTR_STRUCTMAP_TYPE;
    public static final QName QN_ATTR_DIV_ORDER = new QName(EMPTY_NAMESPACE, "ORDER");
    public static final QName QN_ATTR_DIV_LABEL = new QName(EMPTY_NAMESPACE, "LABEL");
    public static final QName QN_ATTR_DIV_ORDERLABEL = new QName(EMPTY_NAMESPACE, "ORDERLABEL");
    
    public static final QName QN_ELE_FPTR = new QName(METS_NAMESPACE, "fptr");
    public static final QName QN_ATTR_FPTR_FILEID = new QName(EMPTY_NAMESPACE, "FILEID");

    
    private static Logger log = LogManager.getLogger(METSParser.class);

    protected File metsFile;
    protected VolumeRecord volumeRecord;
    protected XMLInputFactory factory;
    
    public METSParser(File metsFile, VolumeRecord volumeRecord, XMLInputFactory factory) {
        this.metsFile = metsFile;
        this.volumeRecord = volumeRecord;
        this.factory = factory;
    }
    
    public VolumeRecord getVolumeRecord() {
        return volumeRecord;
    }
    
    
    public void parse() throws FileNotFoundException, XMLStreamException, IOException   {
        
        if (log.isTraceEnabled()) log.trace("parsing METS for volume " + volumeRecord.getVolumeID());
        
        String metsContents = readFileIntoString(metsFile);
        
        volumeRecord.setMETSContents(metsContents);
        
        
        InputStream inputStream = new ByteArrayInputStream(metsContents.getBytes("utf-8"));
        
        XMLStreamReader reader = factory.createXMLStreamReader(inputStream);
        
        int event;
        
        Stack<ParsePhaseEnum> phaseStack = new Stack<ParsePhaseEnum>();
                
        PageRecord currentPageRecord = null;
        PageDivBean currentPageDivBean = null;
        
        do {
            event = reader.next();
            switch (event) {
            case XMLStreamConstants.START_ELEMENT:
            {
                QName qName = reader.getName();
                if (log.isTraceEnabled()) log.trace("qName: " + qName);
                if (phaseStack.isEmpty()) {
                    if (log.isTraceEnabled()) log.trace("**** START_ELEMENT: stack empty");
                    if (QN_ELE_FILEGRP.equals(qName)) {

                        HashMap<QName, String> fileGrpAttributesMap = getAttributes(reader);
                        String attributeValue = fileGrpAttributesMap.get(QN_ATTR_FILEGRP_USE);
                        if (attributeValue != null && "ocr".equals(attributeValue)) {
                            phaseStack.push(ParsePhaseEnum.OCR_FILEGRP);
                        }
                    } else if (QN_ELE_STRUCTMAP.equals(qName)) {
                        HashMap<QName, String> structMapAttributesMap = getAttributes(reader);
                        String typeAttributeValue = structMapAttributesMap.get(QN_ATTR_STRUCTMAP_TYPE);
                        if (typeAttributeValue != null && "physical".equals(typeAttributeValue)) {
                            phaseStack.push(ParsePhaseEnum.STRUCTMAP);
                        }
                        
                    }
                } else {

                    ParsePhaseEnum phase = phaseStack.peek();
                    if (log.isTraceEnabled()) log.trace("**** START_ELEMENT: stack top " + phase.toString());

                    switch (phase) {
                    case OCR_FILEGRP:
                    {
                        if (QN_ELE_FILE.equals(qName)) {
                            HashMap<QName, String> fileAttributesMap = getAttributes(reader);
                            String idAttributeValue = fileAttributesMap.get(QN_ATTR_FILE_ID);
                            if (log.isTraceEnabled()) log.trace("ID: " + idAttributeValue);

                            if (idAttributeValue != null) {
    
                                String sizeAttributeValue = fileAttributesMap.get(QN_ATTR_FILE_SIZE);
                                String seqAttributeValue = fileAttributesMap.get(QN_ATTR_FILE_SEQ);
                                String checksumAttributeValue = fileAttributesMap.get(QN_ATTR_FILE_CHECKSUM);
                                String checksumTypeAttributeValue = fileAttributesMap.get(QN_ATTR_FILE_CHECKSUMTYPE);
                                
                                if (log.isDebugEnabled()) {
                                    log.debug("SIZE: " + sizeAttributeValue);
                                    log.debug("SEQ: " + seqAttributeValue);
                                    log.debug("CHECKSUM: " + checksumAttributeValue);
                                    log.debug("CHECKSUMTYPE: " + checksumTypeAttributeValue);
                                }
                                
                                long size = Long.parseLong(sizeAttributeValue);
                                
                                PageRecord pageRecord = volumeRecord.getPageRecordByID(idAttributeValue);
                                if (pageRecord == null) {
                                    pageRecord = new PageRecord();
                                    pageRecord.setByteCount(size);
                                    pageRecord.setChecksum(checksumAttributeValue, checksumTypeAttributeValue);
                                    pageRecord.setID(idAttributeValue);
                                    pageRecord.setSequence(seqAttributeValue);
                                    
//                                    volumeRecord.addPageRecord(pageRecord); // cannot add yet because xlink:href is not known
                                    
                                } else {
                                    log.warn("Duplicate page ID in METS. ID: " + idAttributeValue + " metsFile: " + metsFile.getPath());
                                }
                                
                                currentPageRecord = pageRecord;
                                
                                phaseStack.push(ParsePhaseEnum.FILE);
                                
                            } else {
                                log.warn("Null ID attribute in METS. metsFile: " + metsFile.getPath());
                            }
                            
                            
                        }
                    } // end bracket for case OCR_FILEGRP:
                    break;
                    
                    case FILE:
                    {
                        if (QN_ELE_FLOCAT.equals(qName)) {
                            HashMap<QName, String> fLocatAttributesMap = getAttributes(reader);
                            String xlinkHrefAttributeValue = fLocatAttributesMap.get(QN_ATTR_FLOCAT_XLINKHREF);
                            if (log.isTraceEnabled()) log.trace("xlink:href: " + xlinkHrefAttributeValue);
                            
                            if (xlinkHrefAttributeValue != null) {
                                if (currentPageRecord != null) {
                                    currentPageRecord.setOcrFilename(xlinkHrefAttributeValue);
                                    volumeRecord.addPageRecord(currentPageRecord);
                                    phaseStack.push(ParsePhaseEnum.FLOCAT);
                                } else {
                                    log.warn("Null currentPageRecord. xlink:href: " + xlinkHrefAttributeValue + " metsFile: " + metsFile.getPath());
                                }
                                
                            } else {
                                log.warn("Null xlink:href. metsFile: " + metsFile.getPath());
                            }
                            
                        }
                    } // end bracket for case FILE:
                    break;
                    
                    case STRUCTMAP:
                    {
                        if (QN_ELE_DIV.equals(qName)) {
                            HashMap<QName, String> divAttributesMap = getAttributes(reader);
                            String divTypeAttributeValue = divAttributesMap.get(QN_ATTR_DIV_TYPE);
                            if (divTypeAttributeValue != null && "volume".equals(divTypeAttributeValue)) {
                                phaseStack.push(ParsePhaseEnum.VOLUME_DIV);
                            }
                        }
                    } // end bracket for case STRUCTMAP:
                    break;
                    
                    case VOLUME_DIV:
                    {
                        if (QN_ELE_DIV.equals(qName)) {
                            HashMap<QName, String> divAttributesMap = getAttributes(reader);
                            String divTypeAttributeValue = divAttributesMap.get(QN_ATTR_DIV_TYPE);
                            if (divTypeAttributeValue != null && "page".equals(divTypeAttributeValue)) {
                                String orderAttrValue = divAttributesMap.get(QN_ATTR_DIV_ORDER);
                                if (orderAttrValue != null) {
                                    int order = Integer.parseInt(orderAttrValue);
                                    
                                    String divLabelAttrValue = divAttributesMap.get(QN_ATTR_DIV_LABEL);
                                    String divOrderLabelAttrValue = divAttributesMap.get(QN_ATTR_DIV_ORDERLABEL);
                                    
                                    currentPageDivBean = new PageDivBean();
                                    currentPageDivBean.setOrder(order);
                                    currentPageDivBean.setLabel(divLabelAttrValue);
                                    currentPageDivBean.setOrderLabel(divOrderLabelAttrValue);
                                    
                                    
                                } else {
                                    log.warn("Missing ORDER in div. metsFile: " + metsFile.getPath());
                                }
                                
                                phaseStack.push(ParsePhaseEnum.PAGE_DIV);
                                
                            } else {
                                log.warn("Missing TYPE in div. metsFile: " + metsFile.getPath());
                            }
                        }
                    } // end bracket for case VOLUME_DIV:
                    break;
                    
                    case PAGE_DIV:
                    {
                        if (QN_ELE_FPTR.equals(qName)) {
                            HashMap<QName, String> fptrAttributesMap = getAttributes(reader);
                            String fileIDAttrValue = fptrAttributesMap.get(QN_ATTR_FPTR_FILEID);
                            if (fileIDAttrValue != null) { // && fileIDAttrValue.matches(OCR_FILEID_REGEX)) {
                                if (currentPageDivBean != null) {
                                    PageRecord pageRecord = volumeRecord.getPageRecordByID(fileIDAttrValue);
                                    if (pageRecord != null) {
                                        int order = currentPageDivBean.getOrder();
                                        pageRecord.setOrder(order);
                                        String orderLabel = currentPageDivBean.getOrderLabel();
                                        pageRecord.setLabel(orderLabel);
                                        String featuresAttrValue = currentPageDivBean.getLabel();
                                        
                                        if (log.isDebugEnabled()) {
                                            log.debug("currentPageDivBean.getOrder() " + order);
                                            log.debug("currentPageDivBean.getOrderLabel() " + orderLabel);
                                            log.debug("currentPageDivBean.getLabel() " + featuresAttrValue);
                                        }

                                        if (featuresAttrValue != null && !"".equals(featuresAttrValue.trim())) {
                                            addPageFeatures(pageRecord, featuresAttrValue);
                                        }
                                        phaseStack.push(ParsePhaseEnum.OCR_FPTR);
                                        
//                                    } else {
//                                        log.warn("Null PageRecord retrieved for FILEID: " + fileIDAttrValue + " metsFile: " + metsFile.getPath());
                                    }
                                } else {
                                    if (log.isDebugEnabled()) log.debug("Null currentPageDivBean for FILEID: " + fileIDAttrValue + " metsFile: " + metsFile.getPath());
                                }
                                
                            }
                            
                        }
                    } // end bracket for case PAGE_DIV:
                    break;
                    
                    } // end switch(phase)
                } // end if (phaseStack.isEmpty());
                
            }// end bracket for case START_ELEMENT:
            break;

            case XMLStreamConstants.END_ELEMENT:
            {
                if (!phaseStack.isEmpty()) {
                    ParsePhaseEnum phase = phaseStack.peek();
                    if (log.isTraceEnabled()) log.trace("**** END_ELEMENT: stack top " + phase.toString());

                    QName qName = reader.getName();
                    if (log.isTraceEnabled()) log.trace("qName: " + qName);
                    if ((QN_ELE_FLOCAT.equals(qName) && ParsePhaseEnum.FLOCAT.equals(phase)) ||
                        (QN_ELE_FILE.equals(qName) && ParsePhaseEnum.FILE.equals(phase)) ||
                        (QN_ELE_FILEGRP.equals(qName) && ParsePhaseEnum.OCR_FILEGRP.equals(phase)))
                    {
                        phaseStack.pop();
                        currentPageRecord = null;
                    } else if ((QN_ELE_FPTR.equals(qName) && ParsePhaseEnum.OCR_FPTR.equals(phase)) ||
                               (QN_ELE_DIV.equals(qName) && (ParsePhaseEnum.PAGE_DIV.equals(phase) || ParsePhaseEnum.VOLUME_DIV.equals(phase))) ||
                               (QN_ELE_STRUCTMAP.equals(qName) && ParsePhaseEnum.STRUCTMAP.equals(phase)))
                    {
                        phaseStack.pop();
                        currentPageDivBean = null;
                    }
                }
            } // end bracket for case END_ELEMENT:
            break;
            
            } // end switch(event)
        } while (event != XMLStreamConstants.END_DOCUMENT);
        
        volumeRecord.updatePageCount();
        
        reader.close();
    }
    
    
    private void addPageFeatures(PageRecord pageRecord, String features) {
        StringTokenizer stringTokenizer = new StringTokenizer(features, ",");
        while (stringTokenizer.hasMoreTokens()) {
            String token = stringTokenizer.nextToken().trim();
            if (token != null && !"".equals(token)) {
                pageRecord.addFeature(token);
            }
        }
    }
    
    
    private HashMap<QName, String> getAttributes(XMLStreamReader reader) {
        HashMap<QName, String> rawAttributesMap = new HashMap<QName, String>();
        int attributeCount = reader.getAttributeCount();
        for (int i = 0; i < attributeCount; i++) {
            QName attributeQName = reader.getAttributeName(i);
            String attributeValue = reader.getAttributeValue(i);
            if (log.isTraceEnabled()) log.trace("attributeQName: " + attributeQName + " attributeValue: " + attributeValue);
            rawAttributesMap.put(attributeQName, attributeValue);
        }
        return rawAttributesMap;
    }
    
    
    
    protected String readFileIntoString(File file) {
        StringBuilder fileContentsBuilder = new StringBuilder();
        BufferedReader bufferedReader = null;
        try {
            char[] buffer = new char[32767];
            bufferedReader = new BufferedReader(new FileReader(file));
            int read = 0;
            
            do {
                read = bufferedReader.read(buffer);
                if (read > 0) {
                    fileContentsBuilder.append(buffer, 0, read);
                }
            } while (read > 0);
        } catch (Exception e) {
            log.error("Failed to read file " + file.getPath(), e);
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    log.error("Unable to close BufferedReader for " + file.getPath(), e);
                }
            }
        }
        
        return fileContentsBuilder.toString();
    }

}

