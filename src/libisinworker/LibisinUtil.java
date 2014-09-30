package libisinworker;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author NaeemM
 */
public class LibisinUtil {
    
    public String writeTempFile(String prefix, String suffix, String content){
        String tempFile = "";
        FileWriter fwContent = null;
        try {
            File dmtOutPutFile = File.createTempFile(prefix, suffix);
            fwContent = new FileWriter(dmtOutPutFile.getAbsoluteFile());
            BufferedWriter bwContent = new BufferedWriter(fwContent);
            bwContent.write(content);
            bwContent.close();
            tempFile = dmtOutPutFile.getAbsolutePath();   
        } catch (IOException ex) {
            Logger.getLogger(LibisinUtil.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                fwContent.close();
            } catch (IOException ex) {
                Logger.getLogger(LibisinUtil.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return tempFile;
    }
}
