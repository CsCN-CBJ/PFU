General Information
-------------------
This project supports the feature of fingerprint upgrade, specifically upgrading 20-byte SHA-1 fingerprints to 32-byte SHA-256 fingerprints.

This file only includes the extended content. For basic usage of the project, please refer to README.md.

Features
--------
1. LFU, PFU, Con-PFU, Sim-PFU upgrade algorithms;
2. A new restore algorithm for the upgraded fingerprints;

Dependencies
------------
rocksdb

Running
-------
1. upgrade fingerprints,
   > destor -u<jobid> -i<upgrade_level> -p"a line as in config file"

2. start a restore task with upgraded fingerprints,
   > destor -n<jobid> /path/to/restore -p"a line as in config file"

Warnings
--------
1. ONLY support one backup and one upgrade: version 0 corresponds to the backup, and version 1 corresponds to the upgrade. You can restore data from both versions, but use different commands(destor -r0 and destor -n1). Other commands are not supported.

2. The backup task is forced to use the origin htable kvstore.

3. File backupversion.count is disabled, see recipestore.c:22
