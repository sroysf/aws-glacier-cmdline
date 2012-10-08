package com.codechronicle.aws.glacier.cmdline;

import jline.console.completer.Completer;
import jline.internal.Configuration;

import java.io.File;
import java.util.List;

import static jline.internal.Preconditions.checkNotNull;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 10/8/12
 * Time: 12:02 AM
 * To change this template use File | Settings | File Templates.
 */
public class CurrentDirAwareFileNameCompleter implements Completer {
    // TODO: Handle files with spaces in them

    private static final boolean OS_IS_WINDOWS;

    static {
        String os = Configuration.getOsName();
        OS_IS_WINDOWS = os.contains("windows");
    }

    private String currentDirectory = null;

    public CurrentDirAwareFileNameCompleter() {
        this.currentDirectory = new File(".").getAbsoluteFile().getParent();
    }

    public int complete(String buffer, final int cursor, final List<CharSequence> candidates) {
        // buffer can be null
        checkNotNull(candidates);

        if (buffer == null) {
            buffer = "";
        }

        if (OS_IS_WINDOWS) {
            buffer = buffer.replace('/', '\\');
        }

        String translated = buffer;

        File homeDir = getUserHome();

        // Special character: ~ maps to the user's home directory
        if (translated.startsWith("~" + separator())) {
            translated = homeDir.getPath() + translated.substring(1);
        }
        else if (translated.startsWith("~")) {
            translated = homeDir.getParentFile().getAbsolutePath();
        }
        else if (!(translated.startsWith(separator()))) {
            String cwd = getWorkingDirectory().getAbsolutePath();
            translated = cwd + separator() + translated;
        }

        File file = new File(translated);
        final File dir;

        if (translated.endsWith(separator())) {
            dir = file;
        }
        else {
            dir = file.getParentFile();
        }

        File[] entries = dir == null ? new File[0] : dir.listFiles();

        return matchFiles(buffer, translated, entries, candidates);
    }

    protected String separator() {
        return File.separator;
    }

    protected File getUserHome() {
        return Configuration.getUserHome();
    }

    protected File getWorkingDirectory() {
        return new File(this.currentDirectory);
    }

    protected int matchFiles(final String buffer, final String translated, final File[] files, final List<CharSequence> candidates) {
        if (files == null) {
            return -1;
        }

        int matches = 0;

        // first pass: just count the matches
        for (File file : files) {
            if (file.getAbsolutePath().startsWith(translated)) {
                matches++;
            }
        }
        for (File file : files) {
            if (file.getAbsolutePath().startsWith(translated)) {
                CharSequence name = file.getName() + (matches == 1 && file.isDirectory() ? separator() : " ");
                candidates.add(render(file, name).toString());
            }
        }

        final int index = buffer.lastIndexOf(separator());

        return index + separator().length();
    }

    protected CharSequence render(final File file, final CharSequence name) {
        return name;
    }

    public String getCurrentDirectory() {
        return currentDirectory;
    }

    public void setCurrentDirectory(String currentDirectory) {
        this.currentDirectory = currentDirectory;
    }
}
