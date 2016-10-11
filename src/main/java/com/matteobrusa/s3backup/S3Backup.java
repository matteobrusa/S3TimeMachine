package com.matteobrusa.s3backup;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.matteobrusa.s3backup.connector.BackupConnector;
import com.matteobrusa.s3backup.connector.LocalBackupConnector;
import com.matteobrusa.s3backup.connector.S3BackupConnector;
import com.matteobrusa.s3backup.connector.S3URI;

public class S3Backup {

	public static boolean dryRun;
	static boolean debug;

	public static void main(String[] args) {

		// CryptoService.setPassword("pazuz");
		// S3Backup.debug=true
		// File from = new File("test.txt");
		// File to = new File("test.txt.aes");
		// File back = new File("back.txt");
		// to.delete();
		// back.delete();
		//
		// try {
		//
		// // EncryptingInputStream eis = new EncryptingInputStream(new
		// // FileInputStream(from));
		//
		// InputStream eis = CryptoService.getEncryptingInputStream(new
		// FileInputStream(from));
		//
		// DataInputStream dis = new DataInputStream(eis);
		//
		// byte[] data = new byte[16];
		//
		// // int n = eis.read(data);
		// // n = eis.read(data);
		// // n = eis.read(data);
		// // dis.readFully(data);
		//
		// Files.copy(eis, to.toPath());
		//
		// // CryptoService.encrypt(from, new FileOutputStream(to));
		//
		// CryptoService.decrypt(new FileInputStream(to), back);
		//
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		// System.exit(0);

		try {
			int poolSize = 2;
			dryRun = false;
			boolean aes128 = false;
			debug = false;
			String password = null;

			S3URI source = null, destination = null;
			String accessKey = null;
			String secretKey = null;
			String include = null;
			String exclude = null;
			String snapshot = null;
			Integer keep = null;

			String s3Credentials = System.getenv("S3CREDENTIALS");

			List<String> list = new LinkedList<String>(Arrays.asList(args));
			boolean valid = true;

			String action = list.get(0);
			list.remove(action);

			for (String s : list) {
				// if (s.startsWith("--")) {
				//
				// if (s.equals("--yes"))
				// prompt = false;
				//
				if (s.equals("--dry-run"))
					dryRun = true;
				else if (s.equals("--aes128"))
					aes128 = true;
				else if (s.equals("--verbose"))
					debug = true;

				// else if (s.startsWith("--threads="))
				// poolSize = Integer.parseInt(s.split("=")[1]);
				//
				// else
				else if (s.startsWith("--include="))
					include = s.split("=")[1];
				else if (s.startsWith("--exclude="))
					exclude = s.split("=")[1];
				else if (s.startsWith("--snapshot="))
					snapshot = s.split("=")[1];
				else if (s.startsWith("--password="))
					password = s.split("=")[1];
				else if (s.startsWith("--credentials="))
					s3Credentials = s.split("=")[1];
				else if (s.startsWith("--keep="))
					keep = new Integer(s.split("=")[1]);
				else if (s.startsWith("--")) {
					System.err.println("Unknown option " + s);
					System.exit(1);
				} else {
					if (source == null)
						source = new S3URI(s);
					else
						destination = new S3URI(s);
				}
			}

			if ((destination != null && destination.isRemote()) || source.isRemote())
				if (s3Credentials == null) {
					valid = false;
					System.err.println("Error: missing credentials in environment and parameters");
				} else {
					String[] array = s3Credentials.split(":");
					if (array.length == 2) {
						accessKey = array[0];
						secretKey = array[1];
					} else {
						valid = false;
						System.err.println("Error: cannot parse credentials '" + s3Credentials + "'");
					}
				}

			if (!valid) {
				help();
				System.exit(1);
			}

			System.out.println("s3 access key: " + accessKey);
			System.out.println("Source: " + source.toString());

			if (destination != null)
				System.out.println("Destination: " + destination.toString());
			// System.out.println("use " + poolSize + " concurrent threads");
			System.out.println("dry run: " + dryRun);
			if (include != null)
				System.out.println("include: " + include);
			if (exclude != null)
				System.out.println("exclude: " + exclude);
			if (password != null)
				System.out.println("encryption: " + (aes128 ? "AES128" : "AES256"));
			if (snapshot != null)
				System.out.println("snapshot: " + exclude);
			if (keep != null)
				System.out.println("keep: last " + keep + " backups");

			Report report = null;
			BackupConnector connector;

			// Backup
			if (action.equals("backup") && validateSourceAndDestination(source, destination)) {
				if (destination.isRemote())
					connector = new S3BackupConnector(destination, accessKey, secretKey);
				else
					connector = new LocalBackupConnector(destination.getPath());

				BackupService service = new BackupService(source.getPath(), connector, debug, include, exclude, password);
				report = service.backup();
				if (report.count == 0) {
					System.out.println("nothing to do... ");
				} else {
					System.out.print("Backed up " + report.count + " files, ");
					System.out.println(
							"transferred " + report.bytes/1024/1024 + " Mbytes from " + report.transferred + " new files in " + report.time/1000 + " seconds.");
				}

				// Restore
			} else if (action.equals("restore") && validateSourceAndDestination(source, destination)) {
				if (source.isRemote())
					connector = new S3BackupConnector(source, accessKey, secretKey);
				else
					connector = new LocalBackupConnector(source.getPath());
				RestoreService service = new RestoreService(connector, destination.getPath(), poolSize, debug, include, exclude, password);
				report = service.restore();

				// List
			} else if (action.equals("list")) {
				if (source.isRemote())
					connector = new S3BackupConnector(source, accessKey, secretKey);
				else
					connector = new LocalBackupConnector(source.getPath());

				ListService service = new ListService(connector, debug, include, exclude);

				if (snapshot == null)
					report = service.listAllBackups();
				else
					report = service.listBackup(snapshot);
			} else if (action.equals("prune")) {
				if (source.isRemote())
					connector = new S3BackupConnector(source, accessKey, secretKey);
				else
					connector = new LocalBackupConnector(source.getPath());

				PruneService service = new PruneService(connector, keep);
				service.prune();
			}

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, " + "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	private static boolean validateSourceAndDestination(S3URI source, S3URI destination) {

		boolean valid = true;
		if (source == null || destination == null) {
			valid = false;
			System.err.println("Error: either source or destination is null");
		} else {
			if (source.isRemote() && destination.isRemote()) {
				valid = false;
				System.err.println("Error: cannot have source and destination both remote");
			}
		}

		return valid;
	}

	private static void help() {
		System.err.println();
		System.err.println("Usage: java -jar S3Backup.jar <action> <params> <source> <destination>");
		System.err.println();
		System.err.println("Actions");
		System.err.println("backup, restore, list, prune");
		System.err.println();
		System.err.println("Parameters:");
		System.err.println("--aes128 use 128 bit AES encryption instead of 256");
		System.err.println(
				"--credentials=XXX:YYY specifies the Amazon S3 credentials. Use preferentially the environment variable S3CREDENTIALS=accessKey:secretKey");
		System.err.println("--include=<regex> only include files matching the regex");
		System.err.println("--exclude=<regex> exclude files matching the regex");
		System.err.println("--dry-run does not change any local or remote file");
		System.err.println("--snapshot=YYYYMMDDTHHMMSS list or restore a specific snapshot");
		System.err.println("--dry-run does not change any local or remote file");
		System.err.println();
		System.err.println("Examples");
		System.err.println();
		System.err.println("Backup and restore to Amazon S3");
		System.err.println("java -jar S3Backup.jar backup --exclude=XXX /Volumes/MyPhotos mybucket:backup/");
		System.err
				.println("java -jar S3Backup.jar restore --snapshot=20160917T213100 --include=DSC_1234.jpg mybucket:backup/ /Volumes/MyPhotos");
		System.err.println();
		System.err.println("AES encrypted backup to local disk");
		System.err.println("java -jar S3Backup.jar backup --password=MyPa55 --exclude=XXX /Volumes/MyPhotos /Volumes/backup/");
		System.err.println();
		System.err.println("List existing backups and their content");
		System.err.println("java -jar S3Backup.jar list mybucket:backup/");
		System.err.println("java -jar S3Backup.jar list --snapshot=20160917T213100 mybucket:backup/");
		System.err.println("");

		// System.err.println("--verbose shows more infos");
		// System.err.println("--yes doesn't ask for confirmation");
		System.err.println();
	}
}
