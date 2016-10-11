package com.matteobrusa.s3timemachine.connector;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public interface BackupConnector {

	final String LATEST = "latest";
	final String FILES = "files";
	final String TREE = "tree";

	Map<String, String> getLatestTree();

	Set<String> getStoredFiles();

	// OutputStream getOutputStream(String file);

	void storeInputStream(String key, InputStream is, Long len);

	void storeSignature(String file);

	void startBackup(String tstamp);

	void completeBackup(String tstamp);

	InputStream readInputStream(String key);
	//

	Set<String> getBackupsList();

	Map<String, String> getTree(String snapshot);

	void deleteBackups(TreeSet<String> oldBackups);

	void deleteFiles(TreeSet<String> toRemove);

	void shutdown();
}
