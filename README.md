# iblocksync 1.0
Sync block devices over the network by creating incremental images.

## Getting started

You have to have an initial copy of the source device on your destination host.
The size of the source device (or file) max not change.

* Copy `iblocksync_remote.py` to the home directory on the remote host(s) and make it executable
* Make sure that your remote user is either root or can sudo (use -s for sudo)
* Make sure that your local user can ssh to the remote host(s) (use -i for a SSH key)
* Invoke:
   `python iblocksync.py [options] [user@]source-host /dev/source [user@]destination-host [/dev/destination]`


* For local usage use 'localhost' as hosts :
    `python iblocksync.py localhost /dev/source localhost /dev/destination`


## Usage
```
iblocksync.py [options] [user@]source-host /dev/source [user@]destination-host /dev/destination

Options:
  -h, --help            show this help message and exit
  -b BLOCKSIZE, --blocksize=BLOCKSIZE
                        block size (bytes, defaults to 1MiB)
  -c COMMENT, --comment=COMMENT
                        add an extra comment
  -i KEYFILE, --id=KEYFILE
                        ssh public key file for both hosts
  --id-source=KEYFILESRC
                        ssh public key file for source host
  --id-destination=KEYFILEDST
                        ssh public key file for destination host
  -p PAUSE, --pause=PAUSE
                        pause between processing blocks, reduces system load
                        (ms, defaults to 0)
  -s, --sudo            use sudo on both hosts (defaults to off)
  --sudo-source         use sudo on the source host (defaults to off)
  --sudo-destination    use sudo on the destination host (defaults to off)
```

## Merge incremental backups and restore a snapshot
```
Usage: iblocksync_restore.py [options] IIMG /dev/destination

Options:
  -h, --help   show this help message and exit
  -f, --force  if destination exists, overwrite without asking.
Restore incremental file image
```

## Licence

Based on  blocksync.py <https://gist.github.com/ramcq/0dc76d494598eb09740f>

* Copyright blocksync.py 2006-2008 Justin Azoff <justin@bouncybouncy.net>
* Copyright blocksync.py 2011 Robert Coup <robert@coup.net.nz>
* Copyright blocksync.py 2012 Holger Ernst <info@ernstdatenmedien.de>
* Copyright blocksync.py 2014 Robert McQueen <robert.mcqueen@collabora.co.uk>

Copyright iblocksync.py 2016 Marek Jacob

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, version 3 of the License only.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
