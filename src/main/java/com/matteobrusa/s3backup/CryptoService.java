
package com.matteobrusa.s3backup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.AlgorithmParameters;
import java.security.MessageDigest;
import java.security.spec.KeySpec;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

public class CryptoService {

	public static SecretKeySpec secret;

	public static void setPassword(String password) {
		try {
			SecretKeyFactory factory;
			factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
			byte[] salt = "S3Backup".getBytes();
			KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, 65536, 256);
			SecretKey tmp = factory.generateSecret(spec);
			secret = new SecretKeySpec(tmp.getEncoded(), "AES");

		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	public static ByteArrayInputStream getEncryptingInputStream(InputStream is, byte[] iv) {

		try {
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			// byte[] ivv = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
			// 15 };
			IvParameterSpec ivParams = new IvParameterSpec(iv);
			cipher.init(Cipher.ENCRYPT_MODE, secret, ivParams);

			// AlgorithmParameters params = cipher.getParameters();
			// byte[] iv =
			// params.getParameterSpec(IvParameterSpec.class).getIV();

			ByteArrayOutputStream os = new ByteArrayOutputStream(1024 * 1024);

			// prefix with IV
			os.write(iv);

			int n = 0;
			byte[] data = new byte[65536];
			while ((n = is.read(data)) > 0) {

				byte[] update = cipher.update(data, 0, n);
				os.write(update);
			}

			byte[] ciphertext = cipher.doFinal();
			os.write(ciphertext);

			is.close();
			// os.close();

			return new ByteArrayInputStream(os.toByteArray());

		} catch (Exception e) {
			throw new IllegalStateException(e);
		}

	}

	public static long encrypt(File from, OutputStream os) {
		long count = 0;
		try {
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			cipher.init(Cipher.ENCRYPT_MODE, secret);

			AlgorithmParameters params = cipher.getParameters();
			byte[] iv = params.getParameterSpec(IvParameterSpec.class).getIV();

			FileInputStream fin = new FileInputStream(from);

			// prefix with IV
			os.write(iv);

			int n = 0;
			byte[] data = new byte[65536];
			while ((n = fin.read(data)) > 0) {

				byte[] update = cipher.update(data, 0, n);
				os.write(update);
				count += n;
			}

			byte[] ciphertext = cipher.doFinal();
			os.write(ciphertext);

			fin.close();
			os.close();

			return count;

		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	public static long decrypt(InputStream is, File to, byte[] storedHash) {

		long count = 0;
		try {

			byte[] iv = new byte[16];
			int m = is.read(iv);
			if (m != 16) {
				is.close();
				throw new IllegalStateException("could not read full IV");
			}

			FileOutputStream fout = new FileOutputStream(to);

			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			cipher.init(Cipher.DECRYPT_MODE, secret, new IvParameterSpec(iv));

			CipherInputStream cin = new CipherInputStream(is, cipher);

			MessageDigest digest = MessageDigest.getInstance("MD5");

			int n = 0;
			byte[] data = new byte[65536];
			while ((n = cin.read(data)) > 0) {

				digest.update(data, 0, n);
				fout.write(data, 0, n);
				count += n;
			}

			cin.close();
			is.close();
			fout.close();

			byte[] hash = digest.digest();

			if (!Arrays.equals(iv, hash))
				throw new IllegalStateException("MD5 hash do not match, file corrupted.");
			if (!Arrays.equals(storedHash, hash))
				throw new IllegalStateException("MD5 hash does not match with file name, file tampered.");

			
			
			return count;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

}
