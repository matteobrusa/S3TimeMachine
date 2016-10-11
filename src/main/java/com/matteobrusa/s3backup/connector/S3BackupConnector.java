package com.matteobrusa.s3backup.connector;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.matteobrusa.s3backup.S3Backup;
import com.matteobrusa.s3backup.SigHelper;

public class S3BackupConnector implements BackupConnector {

	private BasicAWSCredentials awsCredentials;
	private String bucketName, rootPath, backupPath, filePath, nextPath;

	private AmazonS3 s3;
	private String latest;

	// static LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(maxQueueSize);

	private ThreadPoolExecutor threadPool = new ThreadPoolExecutor(1, 50, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(100));
	{
		threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
	}

	public S3BackupConnector(S3URI destination, String accessKey, String secretKey) {

		this.bucketName = destination.getBucket();
		this.rootPath = destination.getPrefix();

		backupPath = rootPath + TREE + "/";
		latest = backupPath + LATEST;
		filePath = rootPath + FILES + "/";

		awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
		s3 = new AmazonS3Client(awsCredentials);

	}

	public HashMap<String, String> getTree(String snapshot) {
		Set<String> objectSummaries = getS3ObjectSummaries(backupPath + snapshot);

		HashMap<String, String> res = new HashMap<String, String>(objectSummaries.size());

		for (String s : objectSummaries) {

			// remove hash
			int p = s.lastIndexOf(SigHelper.HASH_SEPARATOR);
			String hash = s.substring(p + 1);
			s = s.substring(0, p);

			res.put(s, hash);
		}

		return res;
	}

	public HashMap<String, String> getLatestTree() {

		String latestFolder = null;

		GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, latest);

		try {
			S3Object obj = s3.getObject(getObjectRequest);
			S3ObjectInputStream inputStream = obj.getObjectContent();

			latestFolder = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));

		} catch (AmazonS3Exception e) { // no such obj key
			if (e.getErrorCode().equals("NoSuchKey"))
				return new HashMap<String, String>(0);
			else
				throw new IllegalStateException(e);
		}

		return getTree(latestFolder);

	}

	public Set<String> getStoredFiles() {

		Set<String> objectSummaries = getS3ObjectSummaries(filePath.substring(0, filePath.length() - 1));
		return objectSummaries;
	}

	public void storeSignature(final String key) {

		if (S3Backup.dryRun)
			return;

		threadPool.execute(new Runnable() {
			public void run() {

				ByteArrayInputStream is = new ByteArrayInputStream(new byte[0]);

				ObjectMetadata objectMetadata = new ObjectMetadata();
				objectMetadata.setContentLength(0);

				PutObjectRequest req = new PutObjectRequest(bucketName, nextPath + key, is, objectMetadata);

				req.setStorageClass(StorageClass.Standard);

				sendPutRequest(key, req);
			}
		});
	}

	public void startBackup(String tstamp) {
		nextPath = backupPath + tstamp + "/";

		if (S3Backup.dryRun)
			return;

		byte[] data = tstamp.getBytes();
		ByteArrayInputStream is = new ByteArrayInputStream(data);

		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(data.length);

		PutObjectRequest req = new PutObjectRequest(bucketName, latest, is, objectMetadata);
		req.setStorageClass(StorageClass.Standard);
		sendPutRequest(latest, req);
	}

	public void completeBackup(String tstamp) {

	}

	public InputStream readInputStream(String key) {
		GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, filePath + key);
		S3Object obj = s3.getObject(getObjectRequest);
		return obj.getObjectContent();
	}

	public void storeInputStream(String key, InputStream is, Long len) {

		if (S3Backup.dryRun)
			return;

		PutObjectRequest req = null;
		ObjectMetadata objectMetadata = new ObjectMetadata();

		if (len != null)
			objectMetadata.setContentLength(len);

		req = new PutObjectRequest(bucketName, filePath + key, is, objectMetadata);

		req.setStorageClass(StorageClass.StandardInfrequentAccess);

		sendPutRequest(key, req);
	}

	// -------
	private Set<String> getS3ObjectSummaries(final String root) {

		int beginIndex = 1 + root.length();

		TreeSet<String> res = new TreeSet<String>();

		ObjectListing objectListing = s3.listObjects(bucketName, root);

		System.out.print("    Retrieving S3 object list for " + root + " ...");

		while (true) {
			List<S3ObjectSummary> summa = objectListing.getObjectSummaries();
			for (S3ObjectSummary s3ObjectSummary : summa) {

				// verify file size
				String key = s3ObjectSummary.getKey();

				if (key.startsWith(FILES)) {
					// SigHelper sig = new SigHelper(key);
					// long size = sig.getFileSize();
					// if (s3ObjectSummary.getSize() != size) {
					// System.err.println(key + " size mismatch. Do something!");
					// }
					//
					String storageClass = s3ObjectSummary.getStorageClass();
					if (!storageClass.equals("STANDARD_IA")) {
						System.out.println(key + " fixing storage class... ");
						fixStorageClass(key);
					}

				}
				String k = key.substring(beginIndex);

				if (k.length() > 0)
					res.add(k);
			}
			if (objectListing.isTruncated()) {
				System.out.print(".");
				objectListing = s3.listNextBatchOfObjects(objectListing);
			} else
				break;
		}

		System.out.println(" " + res.size() + " files.");

		return res;
	}

	@SuppressWarnings("deprecation")
	private void fixStorageClass(final String key) {

		if (S3Backup.dryRun)
			return;
		// CopyObjectRequest copyObjectRequest= new CopyObjectRequest(bucketName, key, bucketName,
		// key).withStorageClass(StorageClass.StandardInfrequentAccess);
		// s3.copyObject(copyObjectRequest);
		threadPool.execute(new Runnable() {
			public void run() {
				s3.changeObjectStorageClass(bucketName, key, StorageClass.StandardInfrequentAccess);
			}
		});
	}

	private void sendPutRequest(String key, PutObjectRequest req) {
		while (true) {
			try {
				s3.putObject(req);
				break;
			} catch (AmazonClientException e) {
				if (!e.isRetryable() || e.getMessage().contains("Access Denied")) {
					throw (e);
				} else {
					System.out.println("Retrying " + key);
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e1) {
					}
				}
			}
		}
	}

	public Set<String> getBackupsList() {

		TreeSet<String> res = new TreeSet<String>();

		ListObjectsRequest req = new ListObjectsRequest().withBucketName(bucketName).withPrefix(backupPath).withDelimiter("/");
		List<String> aa = s3.listObjects(req).getCommonPrefixes();

		// ObjectListing objectListing =
		// List<S3ObjectSummary> summa = objectListing.getObjectSummaries();
		//
		// for (S3ObjectSummary s3ObjectSummary : summa) {
		// String key = s3ObjectSummary.getKey();
		// res.add(key);
		// }
		for (String s : aa) {
			res.add(s.substring(s.indexOf('/') + 1, s.lastIndexOf('/')));
		}
		return res;
	}

	public void deleteBackups(TreeSet<String> oldBackups) {
		for (String backup : oldBackups) {

			System.out.println("Deleting " + backup + " ...");
			String path = backupPath + backup;
			Set<String> list = getS3ObjectSummaries(path);

			List<KeyVersion> keys = new ArrayList<KeyVersion>();

			for (String key : list) {
				keys.add(new KeyVersion(path + "/" + key));

				if (keys.size() == 100) {

					final List<KeyVersion> batch = keys;
					keys = new ArrayList<KeyVersion>();

					threadPool.execute(new Runnable() {
						public void run() {
							s3.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(batch));
						}
					});
				}
			}

			DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName).withKeys(keys);
			s3.deleteObjects(deleteObjectsRequest);
		}

		System.out.println("Done deleting " + oldBackups.size() + " backups");
	}

	public void deleteFiles(TreeSet<String> toRemove) {

		List<KeyVersion> keys = new ArrayList<KeyVersion>();
		for (String key : toRemove) {
			keys.add(new KeyVersion(filePath + key));
		}

		DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName).withKeys(keys);
		s3.deleteObjects(deleteObjectsRequest);

	}

	public void shutdown() {
		while (threadPool.getActiveCount() > 0) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}

		threadPool.shutdown();
	}

}
