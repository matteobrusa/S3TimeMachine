# S3TimeMachine
A command line tool written in java to backup to Amazon S3 inspired by MacOS' time machine 



Usage: java -jar S3TimeMachine.jar <action> <params> <source> <destination>

Actions
backup, restore, list, prune

Parameters:
--aes128 use 128 bit AES encryption instead of 256
--credentials=XXX:YYY specifies the Amazon S3 credentials. Use preferentially the environment variable S3CREDENTIALS=accessKey:secretKey
--include=<regex> only include files matching the regex
--exclude=<regex> exclude files matching the regex
--dry-run does not change any local or remote file
--snapshot=YYYYMMDDTHHMMSS list or restore a specific snapshot
--dry-run does not change any local or remote file
--verbose shows more infos

Examples

Backup and restore to Amazon S3
java -jar S3TimeMachine.jar backup --exclude=XXX /Volumes/MyPhotos mybucket:backup/
java -jar S3TimeMachine.jar restore --snapshot=20160917T213100 --include=DSC_1234.jpg mybucket:backup/ /Volumes/MyPhotos

AES encrypted backup to local disk
java -jar S3TimeMachine.jar backup --password=MyPa55 --exclude=XXX /Volumes/MyPhotos /Volumes/backup/

List existing backups and their content
java -jar S3TimeMachine.jar list mybucket:backup/
java -jar S3TimeMachine.jar list --snapshot=20160917T213100 mybucket:backup/