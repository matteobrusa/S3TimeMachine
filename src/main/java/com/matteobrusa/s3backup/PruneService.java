package com.matteobrusa.s3backup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

import com.matteobrusa.s3backup.connector.BackupConnector;

public class PruneService extends LoggingService {

	private long startTime;
	private BackupConnector src;

	private Report report = new Report();
	private int keep;

	private static final int maxQueueSize = 100000;

	static LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(maxQueueSize);

	public PruneService(BackupConnector src, int keep) {

		this.src = src;
		this.keep = keep;

	}

	public Report prune() {

		startTime = System.currentTimeMillis();

		ArrayList<String> list = new ArrayList<String>(src.getBackupsList());

		System.out.println(
				list.size() + " backups found, from " + Tools.humanDate(list.get(0)) + " to " + Tools.humanDate(list.get(list.size() - 1)));

		int to = list.size();
		int from = to - keep;
		if (from < 0)
			from = 0;

		TreeSet<String> oldBackups = new TreeSet<String>();

		for (int i = 0; i < from; i++) {
			oldBackups.add(list.get(i));
		}

		log("keeping backups since " + Tools.humanDate(list.get(from)));

		// merge trees
		HashMap<String, String> merge = new HashMap<String, String>(list.size());
		for (int i = from; i < to; i++) {
			String snapshot = list.get(i);

			Map<String, String> tree = src.getTree(snapshot);
			merge.putAll(tree);
		}

		// format as file
		HashSet<String> m2 = new HashSet<String>(merge.size());
		for (Entry<String, String> e : merge.entrySet()) {
			String key = e.getKey();
			SigHelper sig = new SigHelper(key);
			String file = sig.getStoragePath(e.getValue());
			m2.add(file);
		}

		// match
		Set<String> files = src.getStoredFiles();
		TreeSet<String> oldFiles = new TreeSet<String>();

		for (String file : files) {
			if (!m2.contains(file)) {
				System.out.println("to delete: " + file);
				oldFiles.add(file);
			}

		}

		if (!S3Backup.dryRun) {

			log("Deleting " + oldBackups.size() + " old backups...");
			if (oldBackups.size() > 0)
				src.deleteBackups(oldBackups);

			log("Keeping " + m2.size() + " files, deleting: " + oldFiles.size());
			if (oldFiles.size() > 0)
				src.deleteFiles(oldFiles);
		}

		report.time = System.currentTimeMillis() - startTime;

		return report;
	}

}
