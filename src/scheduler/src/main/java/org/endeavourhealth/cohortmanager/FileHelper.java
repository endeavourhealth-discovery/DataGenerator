package org.endeavourhealth.cohortmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileHelper {
    private final static Logger logger = LoggerFactory.getLogger(FileHelper.class);

    public static String readTextFile(Path file) throws Exception {

        try {
            if (!Files.exists(file))
                throw new Exception("Could not find file: " + file.getFileName());

            byte[] encoded = Files.readAllBytes(file);
            return new String(encoded, StandardCharsets.UTF_8);

        } catch (Exception e) {
            throw new Exception("Error reading file: " + file.getFileName(), e);
        }
    }

    public static boolean pathNotExists(Path path) {
        return Files.notExists(path);
    }

    public static boolean pathNotExists(String path) {
        Path pathObject = Paths.get(path);
        return pathNotExists(pathObject);
    }

    public static void createFolder(Path folder) throws IOException {
        Files.createDirectory(folder);
    }

    public static void writeLargeFileToDisk(Path path, String text) throws IOException {

        logger.trace("Writing file to disk: " + path.toString());

        File file = new File(path.toString());

        try (
                FileWriter fileWriter = new FileWriter(file, false);
                BufferedWriter bwr = new BufferedWriter(fileWriter))
        {
            bwr.write(text);

            bwr.flush();
            bwr.close();
        }
    }
}
