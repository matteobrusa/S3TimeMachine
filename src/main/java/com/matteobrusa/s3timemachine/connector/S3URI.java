package com.matteobrusa.s3timemachine.connector;

import java.io.File;

import com.amazonaws.services.s3.AmazonS3Client;

public class S3URI {
	private String bucket;
	private String prefix;
	private String path;
	private boolean isRemote;

	public S3URI(String s) {

		if (!s.endsWith("/"))
			s += "/";
		int p = s.indexOf(':');

		isRemote = (p > 0);
		if (isRemote) {
			String[] array = s.split(":");
			bucket = array[0];
			prefix = array[1];
			if (prefix.equals("/"))
				prefix = "";
			if (prefix.startsWith("/"))
				prefix = prefix.substring(1);
		} else {
			path = s;
		}
	}

	public boolean validate(AmazonS3Client s3) {
		if (isRemote) {
			// TODO: validate S3 bucket
			return true;
		} else {
			File file = new File(path);
			return file.exists();
		}
	}

	public String getPath() {
		return path;
	}

	public String getPrefix() {
		return prefix;
	}

	public String getBucket() {
		return bucket;
	}

	public boolean isRemote() {
		return isRemote;
	}

	@Override
	public String toString() {
		if (isRemote)
			return "bucket '" + bucket + "' prefix '" + prefix + "'";
		else
			return "path '" + path + "'";
	}
}
