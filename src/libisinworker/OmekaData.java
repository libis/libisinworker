
package libisinworker;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author NaeemM
 */
public class OmekaData {
    
    private final Properties omekaServerConfig;
    private final LibisinUtil libisinUtils;
    private List<GeoLocation> locations;
    private List<DigiTool> digitoolimages;
    public  Logger requestLog;
    public int totalRecords;
    public int addedRecords;
    public int updatedRecords;
    public int failedRecords;
    public int validRecords;
    public int invalidRecords;
    
    public JSONObject omekaElements;
    
    public JSONObject typeRelationshipObject;    
    
    
    public OmekaData(Properties omekaServerConfig){
        this.omekaServerConfig = omekaServerConfig;       
        this.libisinUtils = new LibisinUtil();
        this.locations = new ArrayList<>();
        this.digitoolimages = new ArrayList<>();    
        this.typeRelationshipObject = new JSONObject();
    }
        
    public boolean pushDataToOmeka(String records, String setRecordsType, String requestDirectory, String setName) throws IOException{
        
        try {                                   
            libisinUtils.writeFile(requestDirectory + "/" + setName + "_dmtoutput.json",records, false);
            JSONParser parser = new JSONParser();                       
            JSONArray messageBodyobj = (JSONArray) parser.parse(records);
            String type_id = null;                        
                        
            this.totalRecords = messageBodyobj.size();
            this.validRecords = 0;
            this.invalidRecords = 0;
            this.addedRecords = 0;
            this.updatedRecords = 0;
            this.failedRecords = 0;
            
            this.requestLog.log(Level.INFO, "Total records: {0}", messageBodyobj.size());
            System.out.println("-->Total records: " + messageBodyobj.size());
            for(int i=0; i< messageBodyobj.size(); i++){ 
                System.out.println("--->Processing record: " + (i+1));
                this.requestLog.log(Level.INFO, "Processing record: {0}", (i+1));
                JSONObject object = (JSONObject)messageBodyobj.get(i);  
                
                //String type_identifier = "";
                                
                Boolean isValidRecord = false;
                String urlType = null;
                this.requestLog.log(Level.INFO, "record Type: {0}", setRecordsType);                
                switch(setRecordsType){
                    /*  For type 'collection' a record is valid if it contains value for 
                        'collection::dc:title' field. Unlike 'objects', for 'collection' type 
                        validity of all records needs to be checked individually. 
                        (For objects,entities,occurrences, if one record is invalid others are invalid too and vice versa. 
                        Because PUT command, if provided will put 'item_type' value for all records.
                        If not provided, all records will have null value for 'item_type'.)                    
                    */
                    case "collecties":      
                    case "collections":
                        urlType = "collection";
                        if(object.get("element_texts") != null){
                        JSONArray objectElementsArray = (JSONArray) object.get("element_texts");                        
                        if(this.isValidCollection(objectElementsArray))
                            isValidRecord = true;                        
                        }

                    break;                        

                    /*  For type 'objects,entities,occurrences' a record is valid if it has value for 'item_type' field.
                        Value for this field is set by PUT command (PUT,"object",items::item_type),
                        which sets item_type=object for all records. If PUT command is not used,
                        item_type value for all records will be null. Which means all records are
                        invalid, therefore no further processing is required.
                    */
                    case "objecten":      
                    case "objects":      
                    case "entities":      
                    case "entiteiten":
                    case "gebeurtenissen":
                    case "occurrences":
                    case "storage locations":
                    case "bewaarplaatsen":
                    case "places":
                    case "plaatsen":
                    case "list items":                        
                    case "lijstitems":                        
                        urlType = "item";
                        if(object.get("item_type") != null){
                                                                                   
                            /*  Get type_name and type_id only once(i.e. for the first record) and reuse it
                                for next records.
                            */
                            if(type_id == null){                                
                                System.out.println("item_type->" + object.get("item_type").toString().toLowerCase());
                                if(this.omekaElements.containsKey(object.get("item_type").toString().toLowerCase())){
                                    JSONObject itemTypeObject = (JSONObject)this.omekaElements.get(object.get("item_type").toString().toLowerCase());
                                    type_id = itemTypeObject.get("type_id").toString();
                                    isValidRecord = true;
                                    System.out.println("Item type '" + object.get("item_type") + "' exists with type id: "+ type_id);
                                    this.requestLog.log(Level.INFO, "{0}", String.format("Item type '%s' exists with type id: %s", object.get("item_type"), type_id)); 
                                }
                                else{
                                    System.out.println("Item type '" + object.get("item_type") + "' not Found.");
                                    this.requestLog.log(Level.INFO, "Item type '{0}' not Found", object.get("item_type"));                             
                                }                                                                   
                            }
                            else
                                isValidRecord = true;                                                                                                                   
                        }                            
                    break;  
                        
                    default:
                        isValidRecord = false; /* By default a record is invalid.*/
                }                
                
                if(isValidRecord == false){
                    this.requestLog.log(Level.SEVERE, "Invalid record. No further processing for this record");
                    System.out.println("---->Invalid record.");    
                    this.invalidRecords ++;
                    break;
                }                    
                else{
                    System.out.println("---->Valid record.");
                    this.requestLog.log(Level.INFO, "Valid record");
                    this.validRecords ++;
                }
                    

                /*  Process each record. */
                this.requestLog.log(Level.INFO, "Record type: {0}", urlType);
                
                List responeList = this.processRecords(object, type_id, setRecordsType);                 
                if(responeList != null && responeList.size() == 2 && responeList.get(0) != null && responeList.get(1) != null){                    
                    String requestType = responeList.get(0).toString();
                    JSONObject omekaObject = (JSONObject)responeList.get(1);
                                         
                    this.requestLog.log(Level.INFO, "Omeka operation type: {0}", requestType);
                    System.out.println(omekaObject);
                    boolean success = false;
                    switch(requestType){
                        case "ADD":             
                            System.out.println("---->Omeka Operation type: Add");
                            success = this.addItem(omekaObject, urlType);                        
                            break;

                        case "UPDATE":
                            System.out.println("---->Omeka Operation type: Update");
                            success = this.updateItem(omekaObject, urlType);                                
                            break;
                    } 
                    
                    if(success == true){
                        System.out.println("---->"+ requestType +" successful");  
                        this.requestLog.log(Level.INFO, "--{0} successful", requestType);
                        if(requestType.equals("ADD"))
                            this.addedRecords ++;
                        if(requestType.equals("UPDATE"))
                            this.updatedRecords ++;
                        
                    }
                    else{
                        System.out.println("---->"+ requestType +" failed");  
                        this.requestLog.log(Level.SEVERE, "--{0} failed", requestType);
                        this.failedRecords ++;
                    }                    
                    
                }
                else{
                    System.out.println("Processing failed for record number: " + i+1);
                    this.requestLog.log(Level.SEVERE, "Processing failed for record number: {0}", (i+1)); 
                }                

            }
            return true;    
        } catch (ParseException ex) {
            this.requestLog.log(Level.SEVERE, "Omeka record processing failed"); 
            this.requestLog.log(Level.SEVERE, "Exception Message: {0}", ex.getMessage());
            return false;
        }                
    }

    public Boolean isValidCollection(JSONArray objectElementsArray){
        Boolean isValid = false;
        for (Object objectElement : objectElementsArray) {
            JSONObject element = (JSONObject) objectElement; 
            if(element.get("element").toString().trim().equals(this.omekaServerConfig.getProperty("valid_collection").trim()))
                isValid = true;
        }       
        return isValid;
    }
    
    
    /**
     * Checks if an object exists. If exists, sets operation type to UPDATE otherwise to ADD.
     * Checks if elements of object exists. If an element exists, replace element 
     * and element set values with the appropriate values received from Omeka.
     * if an element does not exist, throw an error for that particular element.
     * @param object object information
     * @param type_id
     * @param setRecordsType
     * @return  Type of the operation to be performed (ADD or UPDATE) and modified object
     */ 
    public  List processRecords(JSONObject object, String type_id, String setRecordsType){
        List responseList = new ArrayList();
        String digitToolElement = this.omekaServerConfig.getProperty("digitoolelement");
        
        String objectIdentifier = "";        
        
        /*  Type_id has values for all non collection types. */
        if(type_id != null){
            /*  Add item_type to omeka record.  */
            JSONObject item_type = new JSONObject();    
            item_type.put("id", type_id);
            object.put("item_type", item_type);     
            
           
            /* 
            Java property does not allow to use white space between multiple words 
            of a property name.For example, property for record type 
            'storage locations' cannot be defined as "storage locations_identifier = archive id".
            Therefore we convert these spaces to underscore character. Thus we define
            "storage_locations_identifier = archive id" in property file, and we search
            for 'storage_locations' instead of 'storage locations'.
            */
            setRecordsType = setRecordsType.replaceAll(" ", "_");

            objectIdentifier = this.omekaServerConfig.getProperty(setRecordsType+"_identifier");            
            if(objectIdentifier !=  null && objectIdentifier.length() > 1){
                objectIdentifier = objectIdentifier.substring(0, 1).toUpperCase() + objectIdentifier.substring(1); 
                this.requestLog.log(Level.INFO, "Type identifier :'" + setRecordsType + "_identifier'  found: "+ objectIdentifier +".");                  
            }
            else{
                this.requestLog.log(Level.INFO, "Type identifier :''{0}_identifier'' not found.", setRecordsType); 
                return null;
            }
            
            /*  Add tags to omeka record. Tags will only be added for items (i.e. where type_id is given) */
            this.requestLog.log(Level.INFO, "processing tags");
            JSONArray tags = new JSONArray();            
            if(object.get("tags") != null){
                String tagsArray[] = object.get("tags").toString().split(";");
                if(tagsArray.length > 0){
                    this.requestLog.log(Level.INFO, "tags: {0} ", tagsArray);
                    for (String tag : tagsArray) {
                        JSONObject tagObject = new JSONObject();               
                        tagObject.put("name", tag);
                        tags.add(tagObject);
                    }
                    object.put("tags", tags);                     
                }
                else
                    this.requestLog.log(Level.INFO, "tags: not available");
                 
            }else
                this.requestLog.log(Level.INFO, "tags: null");
                                      
        }
        
        JSONArray objectElementsArray = (JSONArray) object.get("element_texts");                                         
        for (Object objectElement : objectElementsArray) {
            JSONObject element = (JSONObject) objectElement;
            
            String itemType = "";
            String elementName = "";
            
            String elementIdToAdd = null;
            String elementSetIdToAdd = null;
            
            String[] parts = element.get("element").toString().split("::");
            
            if(parts.length > 1){
                itemType = parts[0];
                elementName = parts[1];
            }
            else
                elementName = parts[0];         
                                    
            /* Find elements from correct name space. Element name should contain namespace, that is ':'. */
            if(elementName.contains(":")){
                String[] splitName = elementName.split(":");  
               
            //temp_start
            // Disabled temporary, untill it is complete
            //TODO: make it configureable. Write tests. 
            /* Add relationship type to the original value of the element. */
            /*    
            boolean addRelationshipType = true; 
            //if types are to be added, add them here            
            if(addRelationshipType == true && typeRelationshipObject != null ){                
                if(typeRelationshipObject.containsKey(elementName.toLowerCase())){
                    String originalValue = element.get("text").toString();                         
                    String types = typeRelationshipObject.get(elementName.toLowerCase()).toString();    
                    String changedValue = originalValue + "(" + types.substring(1, types.length()-1) + ")";                    
                    element.put("text", changedValue);
                }                
            }    */      
            //temp_end                  
                
                
                
                /* Set geolocation values. These values will be used later once object has been added or updated. */
                if(splitName[0].equals("geolocation") && libisinUtils.capitalizeFirstLetter(splitName[1]).equals("Address")){
                    System.out.println("---->Processing geolocation for type " + setRecordsType);
                    this.requestLog.log(Level.INFO, "---->Processing geolocation for type {0}", setRecordsType); 
                    
                                                                              
                    if(element.get("text") != null){
                        
                        /* Entities and Places have different data for georeference.
                           Unlike objects, they have data in latitude and longitude.
                           Furthermore, we only process georeference information 
                           when only one pair of latitude and longitude is given. 
                           For items where multiple pairs are given, we skip them.
                           Reason being no support for drawing a multi latitude, longitude 
                           location in Omeka.
                        */
                        if(setRecordsType.equals("entiteiten") || setRecordsType.equals("entities")
                            || setRecordsType.equals("places") || setRecordsType.equals("plaatsen")
                                ){                           
                            String geoReferenceData = element.get("text").toString();
                            Pattern p = Pattern.compile("\\[([^]]*)\\]");
                            Matcher m = p.matcher(geoReferenceData);
                            JSONArray geoRefObjArray = new JSONArray();                            
                            while (m.find()) {
                                String pathArray [] = m.group(1).split(",");
                                if(pathArray.length != 2)
                                    continue;
                                                               
                                JSONObject geoRefDataObj = new JSONObject();
                                geoRefDataObj.put("latitude", pathArray[0]);                               
                                geoRefDataObj.put("longitude", pathArray[1]);                               
                                
                                JSONObject geoRefObj = new JSONObject();
                                geoRefObj.put("georeference", geoRefDataObj);                                                                 
                                geoRefObjArray.add(geoRefObj);                                
                            }
                            this.setGeoLocation(geoRefObjArray.toString());
                            
                        }
                    
                        else
                            this.setGeoLocation(element.get("text").toString());
                    }                                                                                                     
                    else          
                        this.requestLog.log(Level.SEVERE, "----->Geolocation value not given."); 
                    
                    element.clear();
                    continue;
                }        

                String namespaceToFind = splitName[0];
                if(namespaceToFind.equals("dcterms"))
                    namespaceToFind = "dc";                

                if(this.omekaElements.containsKey(namespaceToFind.toLowerCase()))
                {                                          
                    String elementToFind2 = "";
                    if(namespaceToFind.toLowerCase().equals("dc"))
                        elementToFind2 = libisinUtils.capitalizeFirstLetter(splitName[1]);
                    else
                        elementToFind2 = splitName[1].substring(0, 1).toUpperCase() + splitName[1].substring(1);                    

                    /* Digitool urls. Case insenstive. */
                    if(elementToFind2.toLowerCase().equals(digitToolElement.toLowerCase()))
                    {
                        this.requestLog.log(Level.INFO, "---->Processing digitool"); 
                        if(element.get("text") != null)                                                                             
                            this.setDigiToolPid(element.get("text").toString());
                        else          
                            this.requestLog.log(Level.SEVERE, "----->Digitool pid not given"); 

                        element.clear();
                        continue;                                
                    }                        

                    JSONObject elementTypeJsonObject = (JSONObject)this.omekaElements.get(namespaceToFind.toLowerCase());
                    JSONObject elementJsonObject = (JSONObject)elementTypeJsonObject.get("elements");                    
                    if(!elementJsonObject.containsKey(elementToFind2)){
                        this.requestLog.log(Level.SEVERE, "{0}", String.format("Element '%s' not found in namespace(set) %s", elementToFind2, namespaceToFind)); 
                        element.clear();                                                                                                               
                        continue;                                 
                    }

                    this.requestLog.log(Level.INFO, "{0}", String.format("Element '%s' found in namespace(set) %s", elementToFind2, namespaceToFind)); 
                    JSONObject elementObject = (JSONObject)elementJsonObject.get(elementToFind2);
                    if(!elementObject.containsKey("element_id") || !elementObject.containsKey("set_id")){
                        this.requestLog.log(Level.SEVERE, "Error in getting information about element:{0}, movign to next element", elementToFind2);                             
                        System.out.println("Error in getting information about element '" + elementToFind2+ "', moving to next element");
                        element.clear();
                        continue;                                 
                    }

                    //Element found, get element id and set id
                    elementIdToAdd = elementObject.get("element_id").toString();
                    elementSetIdToAdd = elementObject.get("set_id").toString();                        
 
                    // If non collection record and object_id/entity_id is available in the record received from dmt service
                    // type id is not case senstive
                    if(type_id != null && elementToFind2.toLowerCase().equals(objectIdentifier.toLowerCase())){
                            List existResponseList = this.recordExists(elementIdToAdd, element.get("text").toString());
                            // existResponseList.get(0) contains whether record exists in omeka or not.
                            // Possible values are: true, false or null. 
                            // true: if a record is found
                            // false: no record found
                            // null: find record request to omeka was unsuccessfull

                            // existResponseList.get(1) contains identifier of the found record, 
                            // which is needed to make update requst
                            if(existResponseList.get(0) != null){
                                //record found therefore it is an upate operation
                                if((Boolean)existResponseList.get(0) == true){
                                    responseList.add("UPDATE"); 
                                    object.put("id", existResponseList.get(1));
                                }   

                                //record not found therefore it is an add operation
                                if((Boolean)existResponseList.get(0) == false)   
                                    responseList.add("ADD"); 

                            }                        
                            else        
                            {
                                // null value, therefore no further processing for this object.                       
                                this.requestLog.log(Level.SEVERE, "Existing item check faild, no further processing for this item");                                    
                                //it did not return tur or false, infact it returned null;                        
                                break;
                            }                            
                    }                        

                    // If no type_id is given and element to find is 'title', it is collection type
                    if(type_id == null && elementToFind2.toLowerCase().equals("title")){    
                        //Check if collection exists:
                        //1. if multiple collection exists, do not proceed, throw an error
                        //2. if one collection exists, perform update operation
                        //3. if no collection exists, perform add operation

                        List collectionExistResponseList = this.collectionExists(element.get("text").toString());
                        if(collectionExistResponseList.get(0) != null){
                            //collection found therefore it is an upate operation
                            if((Boolean)collectionExistResponseList.get(0) == true){
                                responseList.add("UPDATE"); 
                                object.put("id", collectionExistResponseList.get(1));
                                this.requestLog.log(Level.INFO, "Collection '{0}' found, it is an update operation", element.get("text").toString()); 
                                System.out.println(element.get("text").toString() + " - Collection found, it is an update operation");
                            } 

                            //collection not found therefore it is an add operation
                            if((Boolean)collectionExistResponseList.get(0) == false){
                                this.requestLog.log(Level.SEVERE, "Collection '{0}' not found, it is an add operation", element.get("text").toString()); 
                                System.out.println(element.get("text").toString() + " - Collection not found, it is an add operation");
                                responseList.add("ADD");                         
                            }   

                        }
                        else{
                            this.requestLog.log(Level.SEVERE, "Collection '{0}' exist check faild", element.get("text").toString()); 
                            System.out.println("Collection (" + element.get("text").toString() + ") exist check faild.");
                            break;
                        }
                    }                        

                /*
                 * Replace element name with: element_id and element_set_id. This is done by adding a new json entry and removing the old entry with 
                   element name
                */                             
                JSONObject elementObj = new JSONObject();
                JSONObject elementSetObj = new JSONObject();

                elementObj.put("id", elementIdToAdd);
                elementSetObj.put("id", elementSetIdToAdd);
                element.remove("element");
                element.put("element", elementObj);                  
                element.put("element_set", elementSetObj);
                }
                else{
                    System.out.println("Namespace '" + namespaceToFind + "' does not exists in omeka, moving to next element");
                    this.requestLog.log(Level.SEVERE, "Namespace (element set) not found: {0}", splitName[0]);                        
                    element.clear();                                                    
                }       
    
            }
            else{
                this.requestLog.log(Level.SEVERE, "{0}", String.format("No name space provided: %s", elementName)); 
                element.clear();
            }
                     
        } 
        this.removeEmptyElements(object);
        responseList.add(object);        
        return responseList;
    }
    
    public JSONObject removeEmptyElements(JSONObject object){
        JSONArray objectElementsArrayNew = new JSONArray();
                
        JSONArray objectElementsArray = (JSONArray) object.get("element_texts"); 
        for (Object objectElement : objectElementsArray) {
            JSONObject element = (JSONObject) objectElement;
            if(!element.isEmpty())
                objectElementsArrayNew.add(element);                
        }
        object.remove("element_texts");
        object.put("element_texts", objectElementsArrayNew);
        return object;
    }    
    
    public List collectionExists(String collectionTitle){   
        List responseList = new ArrayList();
        Boolean collectionExists = null;
        String collectionIdentifier = null;     
        JSONArray collectionsArray = (JSONArray)this.getCollections(null);
        if(collectionsArray == null)
            return null;
        
        collectionExists = false;   // Before searching we assume that collection does not exist.
        int counter = 0;
        for (Object collectionObject : collectionsArray) {
            JSONObject collectionObj = (JSONObject) collectionObject;
            
            for (Object collectionElements : (JSONArray) collectionObj.get("element_texts")) {
                JSONObject collectionElement = (JSONObject) collectionElements;
                if(collectionElement.get("text").toString().trim().equals(collectionTitle.trim())){
                    collectionExists = true;
                    collectionIdentifier = collectionObj.get("id").toString();
                    counter++;
                }                    
            }
        }
        // If multiple collections found, it is an error.
        if(collectionExists == true && counter > 1)
            collectionExists = null;
        
        responseList.add(collectionExists);
        responseList.add(collectionIdentifier);
        return responseList;        
    }

    /**
     * Returns collections
     * @param  collectionId  identifier of a collection. If null, values will be returned for all collections
     * @return      collection(s)
     */    
    public JSONArray getCollections(String collectionId){   
        JSONArray responseBody = new JSONArray();
               
	try {                    
            URI uri = this.prepareRequst("collection",collectionId, false, null);             
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget);             
            HttpEntity entity = response.getEntity();           
            JSONParser parser = new JSONParser();
            responseBody = (JSONArray) parser.parse(EntityUtils.toString(entity, "UTF-8"));  
	  } catch (IOException | org.apache.http.ParseException | ParseException ex) {
              this.requestLog.log(Level.SEVERE, "Getting collections from omeka failed: {0}", ex.getMessage());   
	  }    
        return responseBody;
    }      
    
    public List recordExists(String elementId, String recordId){                      
        List responseList = new ArrayList();
        Boolean recordExists = null;
        String recordIdentifier = null;
                
        Map<String,String> quereyParameters = new HashMap<>();
        quereyParameters.put("search", "");
        quereyParameters.put("advanced[0][element_id]", elementId);
        quereyParameters.put("advanced[0][terms]", recordId);
        quereyParameters.put("advanced[0][type]", "is exactly");
        quereyParameters.put("output", "json");
        
	try {                    
            URI uri = this.prepareRequst("item_browse",null, false, quereyParameters);                         
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget);             
            HttpEntity entity = response.getEntity();
            JSONParser parser = new JSONParser();
            JSONObject messageBody = (JSONObject) parser.parse(EntityUtils.toString(entity, "UTF-8"));

            this.requestLog.log(Level.INFO, "Record exists check url: {0}", uri);  
            
            //Note: Browsing items gives only public items, that means for items with private settings we will not
            //be able to find if item exists or not. 
            if(messageBody.get("items") != null){
                int counter = 0;
                // omeka returns 'items' in json format, we assume it is an empty json, that is item does not exist.
                // if json is not empty, later itemExists will be set to true.
                recordExists = false; 
                
                JSONArray existedItems = (JSONArray) messageBody.get("items");            
                for (Object item : existedItems) {
                    if(counter > 0)
                        break;
                    JSONObject objectElement = (JSONObject) item;
                     if(objectElement.get("id") != null){
                         recordIdentifier = objectElement.get("id").toString();
                         recordExists = true;
                         counter ++;   
                     }
                }                    
            }            
                                                
	  } catch (IOException | org.apache.http.ParseException | ParseException ex) {
		this.requestLog.log(Level.SEVERE, "Record exists check failed: {0}", ex.getMessage());  
	  }
        
        responseList.add(recordExists);
        responseList.add(recordIdentifier);
        return responseList;
    }

    public JSONObject getElementByName(String itemType, String elementName){ 
        JSONObject element = new JSONObject();
        Map<String,String> quereyParameters = new HashMap<>();
        quereyParameters.put("item_type", itemType);
        quereyParameters.put("name", elementName);

	try {                    
            URI uri = this.prepareRequst("element",null, false, quereyParameters); 
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget);             
            HttpEntity entity = response.getEntity();
                       
            JSONParser parser = new JSONParser();
            JSONArray messageBody = (JSONArray) parser.parse(EntityUtils.toString(entity, "UTF-8"));
            int counter = 0;
            for (Object message : messageBody) {
                element = (JSONObject) message;                
                if(counter == 0) //exit after getting first element
                    break;
            }
            
	  } catch (IOException | org.apache.http.ParseException | ParseException ex) {
		this.requestLog.log(Level.SEVERE, "Getting elements from omeka failed: {0}", ex.getMessage());  
	  }         
                                
        return element;
    }    
    
    public boolean addItem(JSONObject object, String urlType){
        if(urlType == null){
            System.out.println("Url type not provided. Add item quite.");
            this.requestLog.log(Level.SEVERE, "Url type not provided. Add item quite");
            return false;
        }        
        
        try {
            URI uri = this.prepareRequst(urlType, null, true, null);
            HttpPost httppost = new HttpPost(uri);
            HttpClient httpclient = new DefaultHttpClient();
            httppost.setHeader("Content-type", "application/json");
            httppost.setEntity(new StringEntity(object.toString(), "UTF-8"));
            
            this.requestLog.log(Level.INFO, "--Add operation url: {0}", uri);
            this.requestLog.log(Level.INFO, "--Add operation request Body: {0}", object.toString()); 
            
            HttpResponse response = httpclient.execute(httppost);
            HttpEntity entityResponse= response.getEntity();
            String responseString = EntityUtils.toString(entityResponse, "UTF-8");
                        
            this.requestLog.log(Level.INFO, "--Add operation response code: {0}", response.getStatusLine().getStatusCode());
            this.requestLog.log(Level.INFO, "--Add operation response: {0}", responseString);
            
            if(response.getStatusLine().getStatusCode() != 201){
                System.out.println("---->Add operation failed: " + responseString);
                this.requestLog.log(Level.SEVERE, "--Add operation failed(Message): {0}", responseString);
                this.requestLog.log(Level.SEVERE, "--Add operation failed(Code): {0}", response.getStatusLine().getStatusCode());
                return false;
            }            
            
            /* Add locations if there are any */
            if(this.locations.size() > 0){
                this.requestLog.log(Level.INFO, "Addig locations");                 
                this.addLocation(responseString);                
            }
            
            /* Add images if there are any */
            if(this.digitoolimages.size() > 0){
                this.requestLog.log(Level.INFO, "Addig images"); 
                this.addDigiToolImage(responseString);                                
            }            
            
            return true;
            
        } catch ( UnsupportedEncodingException ex) {
            this.requestLog.log(Level.SEVERE, "Add item unsupported encoding Exception: {0}", ex.getMessage());
            return false;
        } catch (IOException ex) {
            this.requestLog.log(Level.SEVERE, "Add item IO Exception: {0}", ex.getMessage());
            return false;
        }
    }
    
    public boolean updateItem(JSONObject object, String urlType){
        if(urlType == null){
            System.out.println("Url type not provided. Update item quite.");
            this.requestLog.log(Level.SEVERE, "Url type not provided. Update item quite");
            return false;
        }        
        try {
            URI uri = this.prepareRequst(urlType, object.get("id").toString(), true, null);                                    
            HttpPut httpput = new HttpPut(uri);
            HttpClient httpclient = new DefaultHttpClient();
            httpput.setHeader("Content-type", "application/json");
            object.remove("id");
            httpput.setEntity(new StringEntity(object.toString(), "UTF-8"));
            
            this.requestLog.log(Level.INFO, "--Update operation url: {0}", uri);
            this.requestLog.log(Level.INFO, "--Update operation request Body: {0}", object.toString());
            
            HttpResponse response = httpclient.execute(httpput);                               
            HttpEntity entityResponse= response.getEntity();
            String responseString = EntityUtils.toString(entityResponse, "UTF-8");
            this.requestLog.log(Level.INFO, "--Update operation response: {0}", responseString);            
            if(response.getStatusLine().getStatusCode() != 200){
                System.out.println("---->Update operation failed: " + responseString);
                this.requestLog.log(Level.SEVERE, "--Update operation failed(Message): {0}", responseString);
                this.requestLog.log(Level.SEVERE, "--Update operation failed(Code): {0}", response.getStatusLine().getStatusCode());
                return false;
            }            
            
            /* Add locations. */
            this.addLocation(responseString);                
            
            /* Add images. */
            this.addDigiToolImage(responseString);                                
 
            return true;
            
        } catch (UnsupportedEncodingException ex) {
            this.requestLog.log(Level.SEVERE, "--Update item unsupported encoding Exception: {0}", ex.getMessage());
            return false;
            
        } catch (IOException ex) {
            this.requestLog.log(Level.SEVERE, "--Update item IO Exception: {0}", ex.getMessage());
            return false;
        }
    }
    
    public String getItem(String itemId){
        String responseString = "";        
	try {                    
            URI uri = this.prepareRequst("item",itemId, false, null);     
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget); 
            
            HttpEntity entity = response.getEntity();
            responseString = EntityUtils.toString(entity, "UTF-8");
            
	  } catch (IOException | org.apache.http.ParseException ex) {
		this.requestLog.log(Level.SEVERE, "Get item Exception: {0}", ex.getMessage());
	  }                
        return responseString;
    }

    /**
     *
     * @param elementId
     * @return
     */
    public JSONObject getElementByID(String elementId){
        JSONObject element = new JSONObject();
               
	try {                    
            URI uri = this.prepareRequst("element",elementId, false, null);     
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget);             
            HttpEntity entity = response.getEntity();
                       
            JSONParser parser = new JSONParser();
            JSONObject messageBody = (JSONObject) parser.parse(EntityUtils.toString(entity, "UTF-8"));            
            JSONObject elementSet = (JSONObject)messageBody.get("element_set");
            element.put("element_id",messageBody.get("id").toString());                 // Element name  
            element.put("element_name",messageBody.get("name").toString());              // Element name              
            element.put("set_id",elementSet.get("id").toString());                      // Element set id  

	  } catch (IOException | org.apache.http.ParseException | ParseException ex) {
                this.requestLog.log(Level.SEVERE, "Get element by id exception: {0}", ex.getMessage());
                return null;
	  }                
        return element;
    }
        
    public JSONObject getTypesElements(){
        /* Get all types, for each type retrieve its element, 
           for each element get element id,element name and corresponding set id
        */        
        JSONObject typeElements = new JSONObject();
        
        // Add dublin core elements
        JSONObject dcElements = this.getDCElements();
        JSONObject dcTemp = new JSONObject();
        dcTemp.put("type_id", null);
        dcTemp.put("elements", dcElements);
        typeElements.put("dc", dcTemp);     //add Dublin Core elements with type name "dc"
        
        JSONArray responseBody = (JSONArray)this.getResources("item_type", null, null);
        for (Object responseObject : responseBody) {
            JSONObject type = (JSONObject) responseObject;
 
            JSONArray tyepElementsArray = (JSONArray) type.get("elements");                                         
            JSONObject tempType = new JSONObject();
            JSONObject tempElement = new JSONObject();
            
            for (Object objectElement : tyepElementsArray) {
                JSONObject typeElement = (JSONObject) objectElement;
                
                JSONObject elementObj = this.getElementByID(typeElement.get("id").toString());
                if(elementObj == null)
                    continue;

                JSONObject temp = new JSONObject();
                temp.put("element_id", elementObj.get("element_id"));
                temp.put("set_id", elementObj.get("set_id"));
                
                tempElement.put(elementObj.get("element_name"), temp);
            
            }   
            if(tempElement.size() == 0 )
                continue;
            
            tempType.put("type_id", type.get("id"));
            tempType.put("elements", tempElement);
            typeElements.put(type.get("name").toString().toLowerCase(), tempType);                
        } 
        return typeElements;
    }
    
    public JSONObject getDCElements(){   
        JSONObject elements = new JSONObject();
        JSONArray responseBody = this.getElementsBySetId("1");              
        for (Object responseObject : responseBody) {
            JSONObject dcElement = (JSONObject) responseObject;
            
            JSONObject objectElementSet = (JSONObject)dcElement.get("element_set");
            
            JSONObject temp = new JSONObject();
            temp.put("element_id", dcElement.get("id"));
            temp.put("set_id", objectElementSet.get("id"));
            
            elements.put(dcElement.get("name"), temp);
        }
        return elements;
    }     
    
    /**
     * Returns element name and identifier, and identifier of the associated element set
     * @return      element name, element id and element set id
     */  
    /*
    public JSONObject getElements(){   
        JSONObject elements = new JSONObject();
               
	try {                    
            URI uri = this.prepareRequst("element",null, false, null);     
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget);             
            HttpEntity entity = response.getEntity();
                       
            JSONParser parser = new JSONParser();            
            JSONArray messageBody = (JSONArray) parser.parse(EntityUtils.toString(entity, "UTF-8"));            
            for (Object message : messageBody) {
                JSONObject objectElement = (JSONObject) message;
                JSONObject objectElementSet = (JSONObject)objectElement.get("element_set");
                
                JSONObject temp = new JSONObject();                
                temp.put("element_id", objectElement.get("id"));
                temp.put("set_id", objectElementSet.get("id"));
                
                elements.put(objectElement.get("name").toString().toLowerCase(), temp);                
            }
            
	  } catch (IOException | org.apache.http.ParseException | ParseException ex) {
                this.requestLog.log(Level.SEVERE, "Get elements Exception: {0}", ex.getMessage());
                return null;
	  }                
        return elements;
    }   
    */

        
    /**
     * Returns   element set
     * @param    elementSetName name of the element set
     * @return  element set in JSONObject format, if not found return null
     */    
    public JSONObject getElementSetByName(String elementSetName){          
        JSONArray responseBody = (JSONArray) this.getResources("element_set", null, null);
        
        if(responseBody == null)    //do not proceed if no element sets found
            return null;
        
        for (Object responseObject : responseBody) {
            JSONObject elementSet = (JSONObject) responseObject;
            if(elementSet.get("name").equals(elementSetName)){
                return elementSet;
            }                
        }
               
        return null;
    }     

    /**
     *
     * @param elementSetId  identifier of the set for which elements to get
     * @return elements belonging to a set
     */
    public JSONArray getElementsBySetId(String elementSetId){   
        Map<String,String> quereyParameters = new HashMap<>();
        quereyParameters.put("element_set", elementSetId);        
        return this.getResources("element", null, quereyParameters);
    }      

    /**
     *
     * @param elementName   name of the element to search
     * @param elementSetId  identifier of the set where to find element
     * @return  returns element in JSONObject format if found, otherwise returns null
     */
    public JSONObject getElementInSet(String elementName, String elementSetId){ 

        JSONArray elementObjects  = this.getElementsBySetId(elementSetId);
        
        if(elementObjects == null)
            return null;
        
        for (Object elementObject : elementObjects) {
            JSONObject element= (JSONObject) elementObject;
            if(element.get("name").equals(elementName))
                return element;
        }        
                
        return null;
    }    
    
    
    /**
     * Returns  requested resource(s)
     * @param resourceType type of the resource to get, e.g. collection,elements,element sets
     * @param id identifier of the resource, if not provided all records of that resource type will be returned
     * @param quereyParameters  query parameters to add to the url
     * @return  resource(s) in JSONArray
     */    
    public JSONArray getResources(String resourceType, String id, Map<String,String> quereyParameters){   
        JSONArray responseBody = new JSONArray();
               
	try {                    
            URI uri = this.prepareRequst(resourceType,id, false, quereyParameters);                            
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget);             
            HttpEntity entity = response.getEntity();
                       
            JSONParser parser = new JSONParser();
            responseBody = (JSONArray) parser.parse(EntityUtils.toString(entity, "UTF-8"));  
	  } catch (IOException | org.apache.http.ParseException | ParseException ex) {
                this.requestLog.log(Level.SEVERE, "Get resources Exception: {0}", ex.getMessage());
	  }    
        return responseBody;
    }    
    
    
    /**
     * Returns a pair of item type name and identifier
     * @param typeId identifier of an item type     
     * @return  returns a pair of item type id and name of the if no argument is provided
     * values of all existing item types are returned.
     */ 
    public JSONObject getItemTypes(String typeId){   
        JSONObject itemTypes = new JSONObject();
	try {                    
            URI uri = this.prepareRequst("item_type",typeId, false, null);     
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget);             
            HttpEntity entity = response.getEntity();
           
            JSONParser parser = new JSONParser();
            JSONArray messageBody = (JSONArray) parser.parse(EntityUtils.toString(entity, "UTF-8")); 
           
            for (Object message : messageBody) {
                JSONObject objectElement = (JSONObject) message;
                
                itemTypes.put(objectElement.get("name").toString(), objectElement.get("id"));                
            }
            
	  } catch (IOException | org.apache.http.ParseException | ParseException ex) {
                this.requestLog.log(Level.SEVERE, "Get resources Exception: {0}", ex.getMessage());
	  }                
        return itemTypes;
    }     
    
    public HttpResponse addResource(String urlType, String id, boolean useKey, Map<String,String> quereyParameters, String requestBody, String contentType){
        if(urlType == null){
            System.out.println("Url type not provided. Add resource quite.");
            this.requestLog.log(Level.SEVERE, "Url type not provided. Add resource quite");
            return null;
        }        
        
        try {
            URI uri = this.prepareRequst(urlType, id, useKey, quereyParameters);
            HttpPost httppost = new HttpPost(uri);
            HttpClient httpclient = new DefaultHttpClient();
            if(requestBody !=null && contentType != null){
                httppost.setHeader("Content-type", contentType);
                httppost.setEntity(new StringEntity(requestBody, "UTF-8")); 
                this.requestLog.log(Level.INFO, "Add resource request body: {0}", requestBody);
            }            
            this.requestLog.log(Level.INFO, "Add resource url: {0}", uri);
            return httpclient.execute(httppost);
            
        } catch ( UnsupportedEncodingException ex) {
            this.requestLog.log(Level.SEVERE, "Add resource unsupported encoding Exception: {0}", ex.getMessage());
            return null;
        } catch (IOException ex) {
            this.requestLog.log(Level.SEVERE, "Add resource IO Exception: {0}", ex.getMessage());
            return null;
        }
    }
    
    /**
     * Remove existing resources. For example, existing location for an item. 
     * All locations are retrieved and searched for item_id. Found locations are removed.
     * @param id
     * @param resourceType
     */
    public void removeResource(String id, String resourceType){
        this.requestLog.log(Level.INFO, "{0}", String.format("Removing '%s' resources for id: %s", resourceType, id));                 
        JSONArray responseBody = (JSONArray)this.getResources(resourceType, null, null);
        if(responseBody.size() >0 ){
            for (Object objectElement : responseBody) {
                JSONObject element = (JSONObject) objectElement;
                if(!element.isEmpty()){
                    String locationID = element.get("id").toString();                
                    JSONObject locationItem = (JSONObject)element.get("item");                
                    String locationItemID = locationItem.get("id").toString();  
                    if(locationItemID.equals(id) && locationID.length() > 0){
                        try {                                            
                            this.requestLog.log(Level.INFO, "Removing resource with id: {0}", locationID);
                            URI uri = this.prepareRequst(resourceType,locationID, true, null);             
                            HttpDelete httpdelete = new HttpDelete(uri);
                            HttpClient httpclient = new DefaultHttpClient();
                            HttpResponse response = httpclient.execute(httpdelete);   
                            this.requestLog.log(Level.INFO, "Removing resource response: {0}", response);

                          } catch (IOException ex) {
                                this.requestLog.log(Level.SEVERE, "Removing resources Exception: {0}", ex.getMessage());
                          }                                                             
                    }
                }
            }              
        }
        else
            this.requestLog.log(Level.INFO, "Remove ' {0}' failed. Because no information exist in the database.", resourceType);
    }
            
    /**
     * Adds location to a record. 
     * Currently only one location can be added in omeka.
     * Before adding a new location, existing location is removed. 
     * Location will be added if 'locations' variable is not empty. 
     * In case it is empty existing location will be removed, leaving no location in omeka for this record.
     * @param item
     */
    public void addLocation(String item){
        try {
            JSONParser parser = new JSONParser();        
            JSONObject itemObj = (JSONObject) parser.parse(item);
            if(itemObj.get("id") != null){
                                
                //Remove existing location from omeka before adding a new one. 
                //Currently, it is not possible to add multiple locations in omeka
                this.removeResource(itemObj.get("id").toString(), "geo_location");

                // Return if there are no locations to add 
                if(this.locations.isEmpty())
                    return;
                                
                this.requestLog.log(Level.INFO, "Addig locations for: {0}", itemObj.get("id"));
                
                for (GeoLocation geoLocation : this.locations) {
                    JSONObject locationObj = new JSONObject();
                    JSONObject itemObject = new JSONObject();
                    itemObject.put("id", itemObj.get("id"));                     
                                        
                    if(geoLocation.latitude != null)
                        locationObj.put("latitude", geoLocation.latitude);
                    if(geoLocation.longitude != null)
                        locationObj.put("longitude", geoLocation.longitude);
                    if(geoLocation.address != null)
                        locationObj.put("address", geoLocation.address);                
                    if(geoLocation.mapType != null)
                        locationObj.put("map_type", geoLocation.mapType);
                    if(geoLocation.zoomLevel >= 0)
                        locationObj.put("zoom_level", geoLocation.zoomLevel);

                    locationObj.put("item", itemObject);

                    HttpResponse response = this.addResource("geo_location", null, false, null, locationObj.toString(), "application/json");
                    if(response != null){
                        HttpEntity entityResponse= response.getEntity();
                        String responseString = EntityUtils.toString(entityResponse, "UTF-8");
                        this.requestLog.log(Level.INFO, "Add location response: {0}", responseString);                                    

                        if(response.getStatusLine().getStatusCode() != 201){
                            this.requestLog.log(Level.SEVERE, "--Add location faild (Message): {0}", responseString);
                            this.requestLog.log(Level.SEVERE, "--Add location faild(Code): {0}", response.getStatusLine().getStatusCode());
                            System.out.println("---->Add location faild: " + responseString);
                        }
                        else
                            this.requestLog.log(Level.SEVERE, "Location addedd successfully: {0}", responseString);                        
                    }
                    else
                        this.requestLog.log(Level.INFO, "Add location faild. Error in POST request to omeka geolocation");                
                }                
            }
            else
                this.requestLog.log(Level.SEVERE, "Adding location failed. Item id missing.");    
            
        } catch (org.apache.http.ParseException | ParseException | UnsupportedEncodingException ex) {
            this.requestLog.log(Level.SEVERE, "Adding location exception: {0}", ex.getMessage());
            this.locations.clear();
        } catch (IOException ex) {
            this.requestLog.log(Level.SEVERE, "Adding location IO exception: {0}", ex.getMessage());
            this.locations.clear();
        } 
        
        this.locations.clear();
    }
        
    public void setGeoLocation(String locationString){
        if(locationString.toLowerCase().contains("georeference".toLowerCase())){
            this.requestLog.log(Level.INFO, "georeference information: {0}", locationString);
        }            
        else
        {
            System.out.println("Error in adding georeference information.");
            this.requestLog.log(Level.SEVERE, "Error in adding georeference information.");
            this.requestLog.log(Level.SEVERE, "Georeference information incorrect.");
            return;
        }
        
        try {
            JSONParser parser = new JSONParser();        
            JSONArray locationArray = (JSONArray) parser.parse(locationString);
            //int counter = 1;
            for (Object objectItem : locationArray) {
                JSONObject geoLocation = (JSONObject) objectItem;
                if(geoLocation.get("georeference") == null)
                    continue;
                GeoLocation tempLocation = new GeoLocation();
                JSONObject locationItem = (JSONObject)geoLocation.get("georeference");
                                
                if(locationItem.get("label") != null)
                    tempLocation.address = locationItem.get("label").toString();
                if(locationItem.get("latitude") != null)
                    tempLocation.latitude = locationItem.get("latitude").toString();
                if(locationItem.get("longitude") != null)
                    tempLocation.longitude = locationItem.get("longitude").toString();
                
                if(tempLocation.address != null || tempLocation.latitude != null || tempLocation.longitude != null)
                    this.locations.add(tempLocation);

                tempLocation = null;                
                //counter ++;
            }
            
        } catch (ParseException ex) {
            this.requestLog.log(Level.SEVERE, "Parse geolocation exception: {0}", ex.getMessage());
            
        }
    }    
    
    /**
     * Remove existing images related to a record
     * @param itemId
     * @return true if images deleted successfully otherwise false
     */    
    public boolean removeDigiToolImage(String itemId){
        this.requestLog.log(Level.INFO, "{0}", String.format("Removing existing images for id: %s", itemId));                 
        try {                                                     
            URI uri = this.prepareRequst("digi_tool", null, true, null);             
            this.requestLog.log(Level.INFO, "Remove image url: {0}", uri);
            HttpPost httppost = new HttpPost(uri);
            HttpClient httpclient = new DefaultHttpClient();            
            httppost.setHeader("Content-type", "application/json");
            
            JSONObject imageObj = new JSONObject();
            imageObj.put("item_id", itemId);
            imageObj.put("pid", "delete");
            imageObj.put("label", null);                       
            httppost.setEntity(new StringEntity(imageObj.toString(), "UTF-8"));     
            
            HttpResponse response = httpclient.execute(httppost);
            HttpEntity entityResponse= response.getEntity();
            String responseString = EntityUtils.toString(entityResponse, "UTF-8");
            
            if(response.getStatusLine().getStatusCode() == 400 && responseString.contains("Deleted digitool-urls with item_id " + itemId)){
                return true;
            }                
            else{
                this.requestLog.log(Level.INFO, "Remove image failed. Response: {0}", responseString);
                return false;
            }
                
          } catch (IOException ex) {
                this.requestLog.log(Level.SEVERE, "Remove image Exception: {0}", ex.getMessage());
                return false;
          }                                                             
    }
    
    /**
     * Add images to an omeka record
     * All images are removed before adding new images
     * Return if there are no images to add
     * @param item
     */
    public void addDigiToolImage(String item){
        try {        
            JSONParser parser = new JSONParser();
            JSONObject itemObj = (JSONObject) parser.parse(item);
            if(itemObj.get("id") != null){  
                
                boolean imageRemoved = this.removeDigiToolImage(itemObj.get("id").toString());
                if(!imageRemoved)
                    return;               
               
                /* Return if there are no images to add */
                if(this.digitoolimages.isEmpty())                
                    return;
                
                this.requestLog.log(Level.INFO, "Addig images for id {0}", itemObj.get("id").toString()); 
                                
                System.out.println("---->Addig image for: " + itemObj.get("id"));
                for (DigiTool digiToolImage : this.digitoolimages) {                    
                    JSONObject imageObj = new JSONObject();
                    imageObj.put("item_id", itemObj.get("id"));
                    
                    if(digiToolImage.pid != null)
                        imageObj.put("pid", digiToolImage.pid);
                    if(digiToolImage.label != null)
                        imageObj.put("label", digiToolImage.label);                    
                                        
                    if(imageObj.get("pid") != null){    
  
                        // Image(pid) will be added for this item
                        HttpResponse response = this.addResource("digi_tool", null, true, null, imageObj.toString(), "application/json");                        
                        if(response != null){
                            HttpEntity entityResponse= response.getEntity();
                            String responseString = EntityUtils.toString(entityResponse, "UTF-8");
                            this.requestLog.log(Level.INFO, "Add image response: {0}", responseString);                                    

                            if(response.getStatusLine().getStatusCode() == 201){
                                this.requestLog.log(Level.SEVERE, "Image addedd successfully: {0}", responseString);                        
                                System.out.println("---->Image ("+ imageObj.get("pid") +" ) added successfully: ");                                
                            }
                            else 
                            {
                                if(response.getStatusLine().getStatusCode() == 400 && this.parseImageAddResponse((JSONObject)parser.parse(responseString))){
                                    this.requestLog.log(Level.INFO, "--Pid '{0}' already exists", imageObj.get("pid"));
                                    System.out.println("---->Add image faild. Image already exists.");
                                }
                                else{
                                    this.requestLog.log(Level.SEVERE, "--Add image faild (Message): {0}", responseString);
                                    this.requestLog.log(Level.SEVERE, "--Add image faild(Code): {0}", response.getStatusLine().getStatusCode());
                                    System.out.println("---->Add image faild: " + responseString);                                    
                                }
                            }                            
                        }
                        else
                            this.requestLog.log(Level.INFO, "Add image faild. Error in POST request to omeka digitoolurl");                         
                                                
                    }
                    this.requestLog.log(Level.SEVERE, "Adding image failed. Pid missing");
                }                
            }
            else
                this.requestLog.log(Level.SEVERE, "Adding image failed. Item id missing.");            
            
        } catch (ParseException ex) {
            this.requestLog.log(Level.SEVERE, "Adding image exception: {0}", ex.getMessage());
            this.digitoolimages.clear();
        } catch (IOException | org.apache.http.ParseException ex) {
            this.requestLog.log(Level.SEVERE, "Adding image IO exception: {0}", ex.getMessage());
            this.digitoolimages.clear();
        }
        
        this.digitoolimages.clear();
    }
    
    public boolean parseImageAddResponse(JSONObject responseJson ){
        if(responseJson.get("errors") != null){
            JSONObject errorJSON = (JSONObject)responseJson.get("errors");
            if(errorJSON.get("item_id") !=null){
                if(errorJSON.get("item_id").toString().equals("Item already has this pid.")){
                    return true;                           
                }
            }
        }        
        return false;
    }
    
    public void setDigiToolPid(String pidString){                
        if(pidString.length() > 0){            
            DigiTool image = new DigiTool();
            image.pid = pidString;
            this.digitoolimages.add(image);
            image = null;                    
        }
        else
            this.requestLog.log(Level.SEVERE, "Invalid digitool pid given: {0}", pidString);
    }
    
    /**
     * Retrieve digitool records for the given item id
     * Currently this method is only being used in unit tests
     * @param itemId
     * @return
     */
    public List getDigiToolPid(String itemId){          
        Map<String,String> quereyParameters = new HashMap<>();
        quereyParameters.put("item_id", itemId);         
        
        JSONArray responseBody = (JSONArray)this.getResources("digi_tool", null, quereyParameters);
        return responseBody;
    }    
        
    /**
     * Prepare http requst url
     * @param apiType   request type corresponds to endpoints in omeka
     * @param id     
     * @param useKey    if true, key will be added to the url. Key is needed for POST and PUT request,  
     *                  for example for adding items
     * @param quereyParameters
     * @return
     */
    
    public URI prepareRequst(String apiType, String id, boolean useKey, Map<String,String> quereyParameters){
        URIBuilder builder = new URIBuilder();
        URI uri = null;
        String path = null;

        builder.setScheme("http").setHost(this.omekaServerConfig.getProperty("omeka_url_base"));
        switch(apiType){
            case "item":
                path = "/"+ this.omekaServerConfig.getProperty("url_item");               
                break;

            case "item_browse":
                /* Different base url for browse */
                builder.setHost(this.omekaServerConfig.getProperty("omeka_url_browse_base"));                   
                path = "/"+ this.omekaServerConfig.getProperty("url_item_browse");              
                break;                

            case "element":
                path = "/"+ this.omekaServerConfig.getProperty("url_element");              
                break; 
                
            case "element_set":
                path = "/"+ this.omekaServerConfig.getProperty("url_element_set");              
                break; 
                
            case "item_type":
                path = "/"+ this.omekaServerConfig.getProperty("url_"+apiType);               
                break;                
                
            case "collection":
                path = "/"+ this.omekaServerConfig.getProperty("url_collection");                
                break;    
                
            case "geo_location":
                path = "/"+ this.omekaServerConfig.getProperty("url_geo_location");              
                break;     
                
            case "digi_tool":
                path = "/"+ this.omekaServerConfig.getProperty("url_digi_tool");              
                break;                  
        }
        
        try {
            
            if(id != null){
                path += "/" + id;                    
            }
            builder.setPath(path);                
                      
            if(useKey)
                builder.addParameter("key", this.omekaServerConfig.getProperty("key"));
            
            if(quereyParameters != null){
                for(String key : quereyParameters.keySet()){
                    builder.addParameter(key, quereyParameters.get(key));
                }
            }
            
            uri = builder.build();
                        
        } catch (URISyntaxException ex) {
            this.requestLog.log(Level.SEVERE, "Prepare url Exception: {0}", ex.getMessage());
        }
        
        return uri;
    }
            
    ///temp_start

    /**
     * Prepare a list (typeRelationshipObject) containing element name and its relationship type(s). 
     * This list is used later while preparing Omeka records for update/add api call. 
     * @param displayBundle
     * @param typesMap
     */
        public void normalizeDmtData(String displayBundle, Map<String, String> typesMap){
            try {    
                JSONParser parser = new JSONParser();                     
                JSONObject bundleObj = (JSONObject)parser.parse(displayBundle);
                JSONObject bundleBodyobj = (JSONObject)bundleObj.get("bundles");

                Map<String,String> bundleArray = new HashMap<>();                                                
                bundleArray = libisinUtils.jsontoArray(bundleBodyobj);
                for(String key : bundleArray.keySet()){                

                    Map<String, String> bundleElement = libisinUtils.jsontoArray((JSONObject)parser.parse(bundleArray.get(key)));                
                    for(String elementKey : bundleElement.keySet()){    
                        if(elementKey.equals("template")){
                            String keyValue = bundleElement.get(elementKey);                        
                                String tempArrayOne[] = keyValue.split("%");  //delimiter used for type restrictions
                                for (String tempValue : tempArrayOne) {
                                    if(tempValue.contains("restrictToRelationshipTypes")){
                                        String tempArrayTwo[] = tempValue.split("=");
                                        String typeKey = tempArrayTwo[0];
                                        String typeValue = tempArrayTwo[1];
                                        String typeValues[] = typeValue.split("\\|"); // type restriction values delimiter                                   
                                        List<String> typeValuesList = new ArrayList<String>();                                    
                                        for(String s: typeValues){
                                            if(typesMap != null && typesMap.containsKey(s))
                                                typeValuesList.add(typesMap.get(s));  
                                        }

                                        if(typeKey.equals("restrictToRelationshipTypes"))
                                            typeRelationshipObject.put(key, typeValuesList);                                                                      
                                    }                                  
                                }                                                                                                                                                        
                        }
                    }                
                }            

            } catch (ParseException ex) {
                this.requestLog.log(Level.SEVERE, "DMT data normalization failed: {0}", ex.getMessage());
            }
    }    
            
    public String normalizeDmtJson(String strObject, JSONObject types){
        String dmtObject = strObject;
        try {
            JSONParser parser = new JSONParser();            
            JSONArray messageBodyobj = (JSONArray) parser.parse(strObject);
            for(int i=0; i< messageBodyobj.size(); i++){
                JSONObject object = (JSONObject)messageBodyobj.get(i);
                JSONArray objectElementsArray = (JSONArray) object.get("element_texts");
                for (Object objectElement : objectElementsArray) {
                    JSONObject element = (JSONObject) objectElement;
                    String elementText = element.get("text").toString();
                }
            }
            return dmtObject;
        } catch (ParseException ex) {
        }        
        return dmtObject;
    }    
    
    ///temp_end
    
}
