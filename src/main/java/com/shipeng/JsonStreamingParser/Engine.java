package com.shipeng.JsonStreamingParser;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.shipeng.data.Record;

public class Engine {

    public List<Record> readJsonStream(String fileName) throws IOException {
        int adunitId = 0;
        int slotVisibilityId = 0;
        String domain = null;
        int mediaType = 0;
        int categoryId = 0;
        int riskId = 0;
        Integer traqScoreId = 0;
        Integer viewabilityScoreId = 0;
        Integer timestamp = 0;
        
        JsonReader reader = new JsonReader(new InputStreamReader(parse(fileName), "UTF-8"));
        List<Record> records = new ArrayList<Record>();
        // level 1 (top level)
        reader.beginObject();
        if (reader.hasNext()) {
            //level 1
            String l1 = reader.nextName();
            //System.out.println("data: " + l1);
            
            // level 2
            reader.beginObject();
            while (reader.hasNext()) {
                adunitId = 0;
                slotVisibilityId = 0;
                domain = null;
                mediaType = 0;
                categoryId = 0;
                riskId = 0;
                traqScoreId = 0;
                viewabilityScoreId = 0;
                timestamp = 0;
                List<Record> records_tmp = new ArrayList<Record>();
                
                String l2_name = reader.nextName();
                //System.out.println("combo: " + l2_name); // this is where insert statement starts
                String[] tmpArray = l2_name.split("_");
                adunitId         = Integer.parseInt(tmpArray[0]);
                slotVisibilityId = Integer.parseInt(tmpArray[1]);
                domain           = tmpArray[2];
                
                // level 3
                reader.beginObject();
                while (reader.hasNext()) {
                    String l3_name = reader.nextName();
                    //System.out.println("l3_name: " + l3_name);
                    if (l3_name.equals("b") && reader.peek() != JsonToken.NULL) {
                        // level 4
                        reader.beginObject();
                        while (reader.hasNext()) {
                            String l4 = reader.nextName();
                            //System.out.println("media type: " + l4);
                            mediaType = Integer.parseInt(l4);

                            // level 5
                            reader.beginObject();
                            while (reader.hasNext()) {
                                String l5_1 = reader.nextName();
                                int l5_2 = reader.nextInt();
                                //System.out.println("category id: " + l5_1
                                //        + ", " + "risk id: " + l5_2);
                                categoryId = Integer.parseInt(l5_1);
                                riskId     = l5_2;
                                // create a new Record here.
                                Record r = new Record(); 
                                r.setAdunitId(adunitId);
                                r.setSlotVisibilityId(slotVisibilityId);
                                r.setDomain(domain);
                                r.setMediaType(mediaType);
                                r.setCategoryId(categoryId);
                                r.setRiskId(riskId);
                                r.setTraqScoreId(traqScoreId);
                                r.setViewabilityScoreId(viewabilityScoreId);
                                r.setTs(timestamp);
                                
                                records_tmp.add(r);
                            }// end while l5
                            reader.endObject();
                            // end level 5
                        }// end while l4
                        reader.endObject();
                        //end level 4
                    } else if (l3_name.equals("b") && reader.peek() == JsonToken.NULL) {
                        reader.nextNull();
                    } else if (l3_name.equals("t") && reader.peek() != JsonToken.NULL) {
                        int ts = reader.nextInt();
                        //System.out.println("ts: " + ts);
                        timestamp = ts;
                        for (Record r1 : records_tmp) {
                            r1.setTs(timestamp);
                        }
                    } else if (l3_name.equals("t") && reader.peek() == JsonToken.NULL) {
                        reader.nextNull();
                    } else if (l3_name.equals("q") && reader.peek() != JsonToken.NULL) {
                        String q = reader.nextString();
                        //System.out.println("q: " + q);
                        traqScoreId = Integer.parseInt(q);
                        for (Record r1 : records_tmp) {
                            r1.setTraqScoreId(traqScoreId);
                        }
                    } else if (l3_name.equals("v") && reader.peek() != JsonToken.NULL) {
                        String v = reader.nextString();
                        //System.out.println("v: " + v);
                        viewabilityScoreId = Integer.parseInt(v);
                        for (Record r2: records_tmp) {
                            r2.setViewabilityScoreId(viewabilityScoreId);
                        }
                    } else if (l3_name.equals("q") && reader.peek() == JsonToken.NULL) {
                        reader.nextNull();
                        String q = null;
                        //System.out.println("q: " + q);
                        traqScoreId = null;
                    } else if (l3_name.equals("v") && reader.peek() == JsonToken.NULL) {
                        reader.nextNull();
                        String v = null;
                        //System.out.println("v: " + v);
                        viewabilityScoreId = null;
                    }
                }// end while l3
                reader.endObject();
                //end level 3
                
                //add records_tmp to records
                records.addAll(records_tmp);
            }// end while l2
            reader.endObject();
            //end level 2
            
        }//end level 1 if
        reader.endObject();
        //end level 1
        
        reader.close();
        return records;
    }
    
    public void convertJsonToCSV(List<Record> records) {
        try {
            File fout = new File("output.csv");
            FileOutputStream fos = new FileOutputStream(fout);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            String line = null;
            for (Record r : records) {
                line = r.getAdunitId() + "," + r.getSlotVisibilityId() + "," + r.getDomain()
                        + "," + r.getMediaType() + "," + r.getCategoryId() + "," + r.getRiskId()
                        + "," + r.getTraqScoreId() + "," + r.getViewabilityScoreId() + "," 
                        + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(r.getTs() * 1000L));
                bw.write(line);
                bw.newLine();
            }
            bw.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public InputStream parse(String fileName) {
        InputStream result = null;
        
        try {
            //ClassLoader classLoader = getClass().getClassLoader();
            //File file = new File(classLoader.getResource(fileName).getFile());
            File file = new File(fileName);
            if (file.exists()) {
                System.out.println("file exists");
                double sizeInBytes = file.length();
                System.out.println("file size: " + sizeInBytes);
            } else {
                System.out.println("file does not exist");
            }    
            result = new FileInputStream(file);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return result;
    }
    
    public static void main(String[] args) {
        Engine engine1 = new Engine();
        try {
            List<Record> records = engine1.readJsonStream(args[0]);
            engine1.convertJsonToCSV(records);
        }catch (Exception e) {
            e.printStackTrace();
        }
        
    } // end main

} // end class Engine
