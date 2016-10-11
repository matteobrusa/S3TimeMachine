package com.matteobrusa.s3timemachine.service;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

import com.matteobrusa.s3timemachine.Report;
import com.matteobrusa.s3timemachine.SigHelper;
import com.matteobrusa.s3timemachine.connector.BackupConnector;

public class ListService extends LoggingService {

	private long startTime;
	private String include;
	private String exclude;
	private BackupConnector src;

	private Report report = new Report();

	private static final int maxQueueSize = 100000;

	static LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(maxQueueSize);

	public ListService(BackupConnector src, boolean debug, String include, String exclude) {

		this.src = src;
		this.include = include;
		this.exclude = exclude;

	}

	public Report listAllBackups() {

		startTime = System.currentTimeMillis();

		Set<String> backups = src.getBackupsList();

		log("There are " + backups.size() + " available backups.");

		for (String s : backups) {
			log(s);
		}

		report.time = System.currentTimeMillis() - startTime;

		return report;
	}

	public Report listBackup(String snapshot) {
		 Map<String, String> treeMap = src.getTree(snapshot);
		TreeSet<String> tree = new TreeSet<String>(treeMap.keySet());

		applyFilters(tree);

		log("There are " + tree.size() + " files in " + snapshot + " .");

		for (String s : tree) {
			log(s.substring(0, s.lastIndexOf(SigHelper.METADATA_SEPARATOR)));
		}

		return null;
	}

	private void applyFilters(Set<String> localFiles) {

		// use a local copy to work on
		for (String key : new TreeSet<String>(localFiles)) {

			// if not included, remove from list
			if (include != null && !key.toLowerCase().matches(include)) {
				localFiles.remove(key);
			}

			// remove if excluded
			if (exclude != null && key.toLowerCase().matches(exclude)) {
				localFiles.remove(key);
			}
		}

	}

}
