package edu.cmu.raftj.runner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by jiayu on 3/19/15.
 */
public final class Runner {

    private Runner() {
    }

    public static void main(String[] args) {
        checkArgument(args.length == 1, "usage: <config_file_path>");
        String configFilePath = args[0];
        Path path = Paths.get(configFilePath);
        checkArgument(Files.exists(path) && Files.isRegularFile(path) && Files.isReadable(path),
                "%s is not a regular readable file", path);
    }
}
