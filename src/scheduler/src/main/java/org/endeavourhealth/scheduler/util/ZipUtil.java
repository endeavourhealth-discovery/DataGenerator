package org.endeavourhealth.scheduler.util;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

import java.io.File;

public class ZipUtil {

    public static void main(String args[]) {
        try {
            ZipUtil.zipDirectory(new File(args[0]));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void zipDirectory(File sourceLocation) throws Exception {

        System.out.println("Compressing contents of: " + sourceLocation.getAbsolutePath());

        ZipFile zipFile = new ZipFile(sourceLocation.getParent() +
                File.separator +
                sourceLocation.getName() + ".zip");
        System.out.println("Creating file: " + zipFile.getFile().getAbsolutePath());

        // Set the zip file parameters
        ZipParameters parameters = new ZipParameters();
        parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
        parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
        parameters.setIncludeRootFolder(false);

        // Create the multi-part zip file from the files in the
        // specified folder, using the zip file parameters
        zipFile.createZipFileFromFolder(sourceLocation, parameters,
                true, 10485760);

        System.out.println("File successfully created.");
    }

}
