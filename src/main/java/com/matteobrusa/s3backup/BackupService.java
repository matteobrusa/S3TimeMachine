package com.matteobrusa.s3backup;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.matteobrusa.s3backup.connector.BackupConnector;

public class BackupService extends LoggingService {

	private String include;
	private String exclude;
	private String password;

	private String source;
	private BackupConnector dest;
	private Path sourcePath;

	private Report report = new Report();

	public BackupService(String source, BackupConnector dest, boolean debug, String include, String exclude, String password) {

		this.dest = dest;
		this.source = source;

		this.include = include;
		this.exclude = exclude;
		this.password = password;

		sourcePath = Paths.get(source);

		if (password != null)
			CryptoService.setPassword(password);

	}

	public Report backup() {

		long startTime = System.currentTimeMillis();

		Set<File> localFiles = Tools.recurseFS(new File(source));

		applyFilters(localFiles);
		log(localFiles.size() + " files to backup.");

		// prepare tree
		Set<String> localTree = createLocalTree(localFiles);

		// get latest tree
		Map<String, String> remoteTree = dest.getLatestTree();

		// get files in repo
		Set<String> remoteFiles = dest.getStoredFiles();

		// verify backup
		verifyBackup(remoteTree, remoteFiles);

		// create tree on destination
		doSnapshot(localTree, remoteTree, remoteFiles);

		dest.shutdown();

		report.time = System.currentTimeMillis() - startTime;

		return report;
	}

	private void verifyBackup(Map<String, String> remoteTree, Set<String> remoteFiles) {

		for (Iterator<Map.Entry<String, String>> it = remoteTree.entrySet().iterator(); it.hasNext();) {

			Entry<String, String> e = it.next();

			SigHelper sig = new SigHelper(e.getKey());
			String s = sig.getStoragePath(e.getValue());

			if (!remoteFiles.contains(s)) {
				log("WARNING: missing " + s + " from last backup");
				it.remove();
			}
		}
	}

	public void doSnapshot(Set<String> localTree, Map<String, String> remoteTree, Set<String> remoteFiles) {

		String tstamp = Tools.formatDate(new Date());

		dest.startBackup(tstamp);

		int total = localTree.size();
		int counter = 0;
		for (String s : localTree) {

			debugInline(++counter + "/" + total);

			SigHelper sig = new SigHelper(s);

			String originalFilePath = sig.getOriginalFilePath();

			String hash = remoteTree.get(s);
			if (hash != null) { // already there, just save it
				debugInline(originalFilePath + " already exists,");
			} else {

				debugInline("File " + originalFilePath + " is new,");
				byte[] hashArray = Tools.createChecksum(sourcePath.resolve(originalFilePath).toFile());
				hash = Tools.byteArrayToString(hashArray);

				String storagePath = sig.getStoragePath(hash);

				// already in files?
				if (!remoteFiles.contains(storagePath)) {

					Path localPath = sourcePath.resolve(originalFilePath);
					File localFile = localPath.toFile();

					InputStream fis;
					try {
						fis = new FileInputStream(localFile);
					} catch (FileNotFoundException e) {
						throw new IllegalStateException(e);
					}

					Long len = localFile.length();

					// encrypt if needed
					if (password != null) {
						// fis = new EncryptingInputStream(fis);

						ByteArrayInputStream bais = CryptoService.getEncryptingInputStream(fis, hashArray);
						fis = bais;
						len = (long) bais.available();

						debugInline("encrypted,");
					}

					dest.storeInputStream(storagePath, fis, len);

					report.incTransferred();
					report.addBytes(localFile.length());
					debugInline("stored content,");
				} else
					debugInline("deduped,");
			}

			// save the signature in the tree
			storeSignature(sig, hash);
			report.incCount();
			debug("stored signature.");
		}

		dest.completeBackup(tstamp);

	}

	private void storeSignature(final SigHelper sig, final String hash) {

		final String s = sig.appendHash(hash);

		dest.storeSignature(s);

	}

	private Set<String> createLocalTree(Collection<File> collection) {

		TreeSet<String> set = new TreeSet<String>();

		for (File file : collection) {
			String path = sourcePath.relativize(file.toPath()).toString();

			set.add(new SigHelper(path, file).toString());
		}
		return set;
	}

	private void applyFilters(Set<File> localFiles) {

		// use a local copy to work on
		for (File f : new TreeSet<File>(localFiles)) {

			String key = f.getPath();

			// if not included, remove from list
			if (include != null && !key.toLowerCase().matches(include)) {
				localFiles.remove(f);
			}

			// remove if excluded
			if (exclude != null && key.toLowerCase().matches(exclude)) {
				localFiles.remove(f);
			}
		}

	}

}
