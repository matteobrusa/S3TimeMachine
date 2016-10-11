package com.matteobrusa.s3backup;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

public class Tools {

	// public static String fileToKey(String localPath, String remotePath, File
	// file) {
	// String path = file.getPath();
	// if (path.startsWith(localPath))
	// return remotePath + path.substring(localPath.length());
	// else
	// throw new IllegalStateException("path '" + path + "' does not start with
	// localPath '" + localPath + "'");
	// }
	//
	// public static File keyToFile(String localPath, String remotePath, String
	// key) {
	// if (key.startsWith(remotePath)) {
	// String s = key.substring(remotePath.length());
	// return new File(localPath + s);
	// } else
	// throw new IllegalStateException("key '" + key + "' does not start with
	// remotePath '" + remotePath + "'");
	//
	// }

	static byte[] stringToByteArray(String s) {
		byte[] res = new byte[s.length() / 2];

		for (int i = 0; i < s.length(); i += 2) {
			res[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
		}

		return res;
	}

	static String byteArrayToString(byte[] b) {

		String result = "";

		for (int i = 0; i < b.length; i++) {
			result += Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);
		}
		return result;
	}

	public static byte[] createChecksum(File file) {

		try {

			InputStream fis;
			fis = new FileInputStream(file);

			byte[] buffer = new byte[1024];
			MessageDigest digest = MessageDigest.getInstance("MD5");
			int numRead;

			do {
				numRead = fis.read(buffer);
				if (numRead > 0) {
					digest.update(buffer, 0, numRead);
				}
			} while (numRead != -1);

			fis.close();
			return digest.digest();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	public static String getMD5(File file) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");

			FileInputStream fis = null;
			try {
				fis = new FileInputStream(file);

				byte[] dataBytes = new byte[65536];

				int nread = 0;
				while ((nread = fis.read(dataBytes)) != -1) {
					md.update(dataBytes, 0, nread);
				}
				;
				byte[] mdbytes = md.digest();

				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < mdbytes.length; i++) {
					sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
				}

				return sb.toString();
			} finally {
				fis.close();
			}

		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		} catch (FileNotFoundException e) {
			throw new IllegalStateException(e);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	public static Set<File> recurseFS(File parent) {

		Set<File> res = new LinkedHashSet<File>();

		if (parent.isDirectory()) {
			File[] list = parent.listFiles();
			for (File file : list) {
				res.addAll(recurseFS(file));
			}
		} else {
			if (!parent.getPath().endsWith(".DS_Store"))
				res.add(parent);
		}
		return res;
	}

	// public static Map<String, File> recurseFS(String localPath, String
	// remotePath, File parent) {
	//
	// Map<String, File> res = new TreeMap<String, File>();
	//
	// if (parent.isDirectory()) {
	// File[] list = parent.listFiles();
	// for (File file : list) {
	// res.putAll(recurseFS(localPath, remotePath, file));
	// }
	// } else {
	// res.put(fileToKey(localPath, remotePath, parent), parent);
	// }
	// return res;
	// }

	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss");

	public static String formatDate(Date date) {
		return dateFormat.format(date);
	}

	public static String humanDate(String s) {
		int year = new Integer(s.substring(0, 4));
		int month = new Integer(s.substring(4, 6));
		int day = new Integer(s.substring(6, 8));
		String hour =  (s.substring(9, 11));
		String minute = (s.substring(11, 13));
		String second =  (s.substring(13, 15));

		return day + "/" + month + "/" + year + " " + hour + ":" + minute + ":" + second;
	}
}
