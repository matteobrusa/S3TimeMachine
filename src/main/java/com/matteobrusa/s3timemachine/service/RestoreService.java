package com.matteobrusa.s3timemachine.service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

import com.matteobrusa.s3timemachine.Report;
import com.matteobrusa.s3timemachine.SigHelper;
import com.matteobrusa.s3timemachine.Tools;
import com.matteobrusa.s3timemachine.connector.BackupConnector;

public class RestoreService extends LoggingService {

	private long startTime;
	private String include;
	private String exclude;
	private String password;
	private String dest;
	private BackupConnector src;
	private Path destPath;
	private Report report = new Report();

	private static final int maxQueueSize = 100000;

	static LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(maxQueueSize);

	public RestoreService(BackupConnector src, String dest, int poolSize, boolean debug, String include, String exclude, String password) {

		this.dest = dest;
		this.src = src;
		this.include = include;
		this.exclude = exclude;

		destPath = Paths.get(dest);

		this.password = password;
		if (password != null)
			CryptoService.setPassword(password);
	}

	public Report restore() {

		startTime = System.currentTimeMillis();

		Map<String, String> remoteTree = src.getLatestTree();

		TreeSet<String> treeSet = new TreeSet<String>(remoteTree.keySet());
		applyFilters(treeSet);

		for (String s : treeSet) {

			String hash = remoteTree.get(s);

			SigHelper sig = new SigHelper(s);

			String originalFilePath = sig.getOriginalFilePath();

			String storagePath = sig.getStoragePath(hash);

			debug("restoring " + originalFilePath + " to " + dest);

			Path localPath = destPath.resolve(originalFilePath);
			Path parent = localPath.getParent();
			if (parent != null)
				parent.toFile().mkdirs();

			InputStream is = src.readInputStream(storagePath);

			long n;

			try {
				if (password != null) {
					n = CryptoService.decrypt(is, localPath.toFile(), Tools.stringToByteArray(hash));
				} else {
					n = Files.copy(is, localPath);
				}
				is.close();
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}

			report.addBytes(n);
			report.incCount();
			report.incTransferred();

		}

		report.time = System.currentTimeMillis() - startTime;

		return report;
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
