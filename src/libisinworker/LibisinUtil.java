package libisinworker;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import org.apache.commons.io.FilenameUtils;
import org.json.simple.JSONObject;

/**
 *
 * @author NaeemM
 */
public class LibisinUtil {
         
    public String writeFile(String filePath, String content, boolean append){       
        try {
            File file = new File(filePath);
            FileWriter fwContent = new FileWriter(file.getAbsoluteFile(), append);
            BufferedWriter bwContent = new BufferedWriter(fwContent);
            bwContent.write(content);
            bwContent.close();
            return file.getAbsolutePath();
        } catch (IOException ex) {
            Logger.getLogger(LibisinUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }  
    
    public String createRequestDirectory(){                
        String strNanoTime = Long.toString(System.nanoTime());
        String currentWorkingDir = System.getProperty("user.dir");
        String timeName = strNanoTime.substring(strNanoTime.length()/2);        
        return this.createDirectory(currentWorkingDir + "/files", timeName);
        
    }
          
    public String createDirectory(String path, String name){       
        String directoryPath = null;
	File directory = new File(path+ "/" + name);
	if (!directory.exists()) {
		if (directory.mkdir()) {			
                        directory.setWritable(true);
                        directoryPath = directory.getAbsolutePath();                        
		} else {
			System.out.println("Failed to create directory: " + path + "/" + name);
		}
	}
        else
            System.out.println("Directory already exists: " + path + "/" + name);
        
        return directoryPath;
    }    

    public boolean removeFile(String filePath){
        File file = new File(filePath);
        return file.delete();
    }
    
    public Logger createLogger(String logFilePath, String className){                                
        Logger logger = null;
        if(logFilePath == null){            // in case of null it is a server log
            logFilePath = System.getProperty("user.dir") + "/log/default.log";        
            //logFilePath = currentDir + "log/default.log";        
            this.removeFile(logFilePath);   // delete log file at startup
        }

        try {            
            logger = Logger.getLogger(className);
            FileHandler logHandler = new FileHandler(logFilePath, true);
            logHandler.setFormatter(new SimpleFormatter());    
            logger.addHandler(logHandler);
            logger.setUseParentHandlers(false);  
        } catch (IOException | SecurityException ex) {
            System.out.println("Error in removing existing log file, remove it and try again.");
            Logger.getLogger(LibisinUtil.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }
        
        return logger;
    } 
    
    public void destroyLogger(Logger log){
        for (Handler h : log.getHandlers()) {
            h.close();
        }        
    }
        
    public String prettyPrintJson(String jsonString){
        JsonParser parser = new JsonParser();
        JsonObject json = parser.parse(jsonString).getAsJsonObject();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String prettyJson = gson.toJson(json);
        return prettyJson;
    }
    
    public void sendEmail(Properties config, String toEmail, String toName, String fileLog, String fileReport, Logger requestLog){                    
        try
        {   
            String messageText = "Beste "+ toName + ",\n" 
                    + config.getProperty("mail_message");
            Properties props = System.getProperties();
            props.put("mail.smtp.host", config.getProperty("mail_smtp_server"));
 
            Session session = Session.getInstance(props, null);            
            MimeMessage msg = new MimeMessage(session);
            
            msg.addHeader("Content-type", "text/HTML; charset=UTF-8");
            msg.setFrom(new InternetAddress(config.getProperty("mail_from_email"), config.getProperty("mail_from_name")));
            msg.setReplyTo(InternetAddress.parse(config.getProperty("mail_from_email"), false));
            msg.setSubject(config.getProperty("mail_subject"), "UTF-8");
                      
            BodyPart messageBodyPart = new MimeBodyPart();
            messageBodyPart.setText(messageText);
            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(messageBodyPart);
            
            /* Attach report file */
            if(fileReport != null){
                messageBodyPart = new MimeBodyPart();
                DataSource reportSource = new FileDataSource(fileReport);            
                messageBodyPart.setDataHandler(new DataHandler(reportSource));
                messageBodyPart.setFileName("Omeka_Integation_Report.txt");
                multipart.addBodyPart(messageBodyPart);  
                msg.setContent(multipart);     
                requestLog.log(Level.INFO, "Report file attached: {0}", fileReport);  
            }
            
            /* Attach log file */
            boolean isZip = false;
            if(fileLog != null){
                File logFiletoAttach = new File(fileLog); 
                
                /* Compress log file if its size is greater than 5mb */ 
                if(logFiletoAttach.length() > 5242880){ 
                    requestLog.log(Level.INFO, "Log file size ({0} MB) is greater than 5 MB, therefore compressing it.", logFiletoAttach.length()/1048576);
                    String baseFileName = FilenameUtils.getBaseName(logFiletoAttach.getAbsolutePath());
                    String requestDirectory = FilenameUtils.getFullPath(logFiletoAttach.getAbsolutePath());
                    String zipFile = FilenameUtils.concat(requestDirectory, baseFileName+".zip");
                    this.compressFile(logFiletoAttach, zipFile);
                    fileLog = zipFile;    
                    isZip = true;
                    requestLog.log(Level.INFO, "Log file zipped to: {0}", zipFile);
                }

                messageBodyPart = new MimeBodyPart();
                DataSource logSource = new FileDataSource(fileLog);                            
                messageBodyPart.setDataHandler(new DataHandler(logSource));
                if(isZip)
                    messageBodyPart.setFileName("Omeka_Integation_Log.zip");
                else
                    messageBodyPart.setFileName("Omeka_Integation_Log.txt");
                
                multipart.addBodyPart(messageBodyPart);  
                msg.setContent(multipart);         
                requestLog.log(Level.INFO, "Log file attached: {0}", fileLog);                
            }            
            
            msg.setSentDate(new Date());
            msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail, false));
            Transport.send(msg); 
            
            System.out.println("EMail Sent to '" + toName + "', at email: " + toEmail);
            requestLog.log(Level.INFO, "Email sent to: {0}", toName);
            requestLog.log(Level.INFO, "Email sent at: {0}", toEmail);            
        }
        catch (MessagingException | UnsupportedEncodingException ex) {
            System.out.println("EMail could not be sent to '" + toName + "', at email: " + toEmail);
            requestLog.log(Level.SEVERE, "Exception in sending email: {0}", toEmail);
            requestLog.log(Level.SEVERE, "Exception message: {0}", ex.getMessage());
        }    
        
    }    
    
    public String capitalizeFirstLetter(String original){      
    String[] wordArray = original.split(" ");
    
    StringBuilder sb = new StringBuilder();
    for (String word : wordArray) {
        sb.append(word.substring(0, 1).toUpperCase()).append(word.substring(1));
        sb.append(" ");
    }
      return sb.toString().trim();
    }    
        
    public void executeCommand(String command, Logger serverLog){
        String s = null;
 
        try {
            // using the Runtime exec method:
            Process p = Runtime.getRuntime().exec(command);             
            BufferedReader stdInput = new BufferedReader(new
                 InputStreamReader(p.getInputStream()));
            BufferedReader stdError = new BufferedReader(new
                 InputStreamReader(p.getErrorStream()));
             
            // read the output from the command
            serverLog.log(Level.INFO, "Executing command: {0}", command);
            while ((s = stdInput.readLine()) != null) {
                serverLog.log(Level.INFO, "Command output: {0}", s);
            }
             
            // read any errors from the attempted command
            serverLog.log(Level.INFO, "Errors in executing command (if any): {0}", command);
            while ((s = stdError.readLine()) != null) {
                serverLog.log(Level.INFO, "Command output: {0}", s);
            }             
        }
        catch (IOException ex) {
            serverLog.log(Level.SEVERE, "Exception while executing command: {0}", command);
            serverLog.log(Level.SEVERE, "Exception message: {0}", ex.getMessage());
            serverLog.log(Level.SEVERE, "Exception terminated:");
            System.exit(-1);
        }
    }
    
    public void compressFile(File file, String zipFileName) {
        try {
            //create ZipOutputStream to write to the zip file
            FileOutputStream fos = new FileOutputStream(zipFileName);
            ZipOutputStream zos = new ZipOutputStream(fos);
            //add a new Zip Entry to the ZipOutputStream
            ZipEntry ze = new ZipEntry(file.getName());
            zos.putNextEntry(ze);
            //read the file and write to ZipOutputStream
            FileInputStream fis = new FileInputStream(file);
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) > 0) {
                zos.write(buffer, 0, len);
            }
             
            //Close the zip entry to write to zip file
            zos.closeEntry();
            //Close resources
            zos.close();
            fis.close();
            fos.close();            
             
        } catch (IOException e) {
            e.printStackTrace();
        }
    }    
    
  public boolean isValidJSON(String jsonString) {
      Gson gson = new Gson();
      try {
          gson.fromJson(jsonString, Object.class);
          return true;
      } catch(com.google.gson.JsonSyntaxException ex) { 
          return false;
      }
  }
  
  public Map<String,String> jsontoArray(JSONObject object){
    Map<String,String> array = new HashMap<>();      
    Iterator iterator = object.keySet().iterator();
    while(iterator.hasNext()) {
        String key = (String) iterator.next();
        array.put(key, object.get(key).toString());
    }     
    return array;
  }
  
}