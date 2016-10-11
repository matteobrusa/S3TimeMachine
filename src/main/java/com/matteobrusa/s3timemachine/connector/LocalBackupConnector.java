package com.matteobrusa.s3timemachine.connector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.matteobrusa.s3timemachine.S3TimeMachine;
import com.matteobrusa.s3timemachine.SigHelper;
import com.matteobrusa.s3timemachine.Tools;

public class LocalBackupConnector implements BackupConnector {

	File rootDir, backupsDir, filesDir, latestDir, nextDir;
	private Path latestFile;

	public LocalBackupConnector(String root) {

		rootDir = new File(root);
		if (!rootDir.exists())
			throw new IllegalStateException("root " + rootDir + " not found.");
		backupsDir = new File(rootDir, TREE);
		filesDir = new File(rootDir, FILES);

		latestFile = new File(backupsDir, LATEST).toPath();

		if (backupsDir.exists() && filesDir.exists()) {
			readLatestBackupFolder();
		}
	}

	public void startBackup(String tstamp) {

		if (S3TimeMachine.dryRun)
			return;

		if (!backupsDir.exists() && !filesDir.exists()) {

			System.out.println("creating new backup target in " + rootDir);

			backupsDir.mkdir();
			filesDir.mkdir();

			saveLatest("none");
		}

		readLatestBackupFolder();

		nextDir = new File(backupsDir, tstamp);
		nextDir.mkdir();

		saveLatest(tstamp);
	}

	private void readLatestBackupFolder() {
		byte[] bytes = null;
		try {
			bytes = Files.readAllBytes(latestFile);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
		String latest = new String(bytes).trim();

		if (latest.equals("none")) {
			latestDir = null;
		} else {
			latestDir = new File(backupsDir, latest);
			if (!(latestDir.exists() && latestDir.isDirectory())) {
				throw new IllegalStateException("backup folder " + latestDir.getPath() + " does not exist.");
			}
		}
	}

	private void saveLatest(String dir) {
		try {
			Files.write(latestFile, dir.getBytes());
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	public Set<String> getBackupsList() {

		Set<String> res = new TreeSet<String>();

		File[] list = backupsDir.listFiles();
		for (File file : list) {
			res.add(file.getName());
		}

		return res;
	}

	public  Map<String, String> getTree(String snapshot) {

		
		File snapshotFolder = new File(backupsDir, snapshot);
		Path snapshotPath = snapshotFolder.toPath();
		
		Set<File> tree = Tools.recurseFS(snapshotFolder);
		 Map<String, String> res = new TreeMap<String, String>();

		for (File f : tree) {

			String s = snapshotPath.relativize(f.toPath()).toString();

			// remove hash
			int p = s.lastIndexOf(SigHelper.HASH_SEPARATOR);
			String hash = s.substring(p + 1);
			s = s.substring(0, p);

			res.put(s, hash);
		}

		return res;
	}

	public  Map<String, String> getLatestTree() {

		if (latestDir == null)
			return new TreeMap<String, String>();

		return getTree(latestDir.getName());
	}

	public Set<String> getStoredFiles() {

		Set<File> files = Tools.recurseFS(filesDir);

		TreeSet<String> res = new TreeSet<String>();
		for (File file : files) {
			String s = filesDir.toPath().relativize(file.toPath()).toString();
			res.add(s);
		}

		return res;
	}

	// public long storeFile(Path source, String file) {
	// try {
	// Path dest = Paths.get(filesDir.getPath());
	//
	// Path target = dest.resolve(file);
	// target.getParent().toFile().mkdirs();
	// Files.copy(source, target);
	//
	// return target.toFile().length();
	// } catch (IOException e) {
	// throw new IllegalStateException(e);
	// }
	// }
	//
	// public OutputStream getOutputStream(String file) {
	// Path dest = Paths.get(filesDir.getPath());
	//
	// Path target = dest.resolve(file);
	// target.getParent().toFile().mkdirs();
	//
	// try {
	// return new FileOutputStream(target.toFile());
	// } catch (FileNotFoundException e) {
	// throw new IllegalStateException(e);
	// }
	// }

	public void storeSignature(String file) {

		if (S3TimeMachine.dryRun)
			return;

		File f = new File(nextDir, file);
		f.getParentFile().mkdirs();
		try {
			new FileOutputStream(f).close();
		} catch (FileNotFoundException e) {
			throw new IllegalStateException(e);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	public void completeBackup(String tstamp) {

	}

	public void restoreFile(String source, Path filePath) {

		if (S3TimeMachine.dryRun)
			return;

		try {
			Path src = Paths.get(filesDir.getPath()).resolve(source);

			Path parent = filePath.getParent();
			if (parent != null)
				parent.toFile().mkdirs();
			Files.copy(src, filePath);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	public InputStream readInputStream(String source) {
		Path src = Paths.get(filesDir.getPath()).resolve(source);
		try {
			return new FileInputStream(src.toFile());
		} catch (FileNotFoundException e) {
			throw new IllegalStateException(e);
		}
	}

	public void storeInputStream(String key, InputStream is, Long len) {

		if (S3TimeMachine.dryRun)
			return;

		Path dest = Paths.get(filesDir.getPath());

		Path target = dest.resolve(key);
		target.getParent().toFile().mkdirs();

		try {
			Files.copy(is, target, StandardCopyOption.REPLACE_EXISTING);

			is.close();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	public InputStream getInputStream(String source) {
		// TODO Auto-generated method stub
		return null;
	}

	public void deleteBackups(TreeSet<String> oldBackups) {
		// TODO Auto-generated method stub
		
	}

	public void deleteFiles(TreeSet<String> toRemove) {
		// TODO Auto-generated method stub
		
	}

	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

}
