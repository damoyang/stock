package utils;

import java.io.File;
import java.io.FilenameFilter;
import java.util.*;

public class FileProcess {
	private static final String FINISH_FLAG = "_done";

	public static List<File> getFiles(String dirName) {
		List<File> files = new ArrayList<File>();
		File dir = new File(dirName);
		if (!dir.isDirectory()) {
			files.add(dir);
		} else {
			Queue<File> dirs = new LinkedList<File>();
			dirs.offer(dir);
			while (!dirs.isEmpty()) {
				File dirFile = dirs.poll();
				if (dirFile.isDirectory()) {
					File[] documents = getFileList(dirFile);
					for(File f:documents){
						dirs.add(f);
					}
				}else {
					files.add(dirFile);
				}
			}
		}
		return files;
	}

	private static File[] getFileList(File dir) {
		File[] files = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				if (name.contains(FINISH_FLAG))
					return false;
				return true;
			}
		});
		if (files.length == 0)
			return files;
		else {
			Arrays.sort(files, new Comparator<File>() {
				public int compare(File f1, File f2) {
					return f1.getName().compareTo(f2.getName());
				}
			});
			return files;
		}
	}

	public static void rename(File file) {
		// from "xx.csv" to "xx.csv_done", and this file will not be processed
		// again.
		String fileName = file.getPath();
		file.renameTo(new File(fileName + FINISH_FLAG));
	}
}
