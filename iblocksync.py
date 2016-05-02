#!/usr/bin/env python
"""
Sync block devices over the network by creating incremental images.

Getting started:

* Copy iblocksync_remote.py to the home directory on the remote host(s) & make it executable
* Make sure that your remote user is either root or can sudo (use -s for sudo)
* Make sure that your local user can ssh to the remote host(s) (use -i for a SSH key)
* Invoke:
   python iblocksync.py [options] [user@]source-host /dev/source [user@]destination-host [/dev/destination]


* For local usage use `localhost' as hosts :
    python iblocksync.py localhost /dev/source localhost /dev/destination


Based on  blocksync.py <https://gist.github.com/ramcq/0dc76d494598eb09740f>
Copyright blocksync.py 2006-2008 Justin Azoff <justin@bouncybouncy.net>
Copyright blocksync.py 2011 Robert Coup <robert@coup.net.nz>
Copyright blocksync.py 2012 Holger Ernst <info@ernstdatenmedien.de>
Copyright blocksync.py 2014 Robert McQueen <robert.mcqueen@collabora.co.uk>
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

"""
__version__ = '1.0'

import os
import sys
LEN_HASH = 20 # length of used hash in bytes
# Keep LEN_HASH in sync with iblocksync_remote.
import subprocess
import time
import abc
import json
import iblocksync_remote

class Communicator(object):
    """Provides basic methods to communicate with a remote iblocksync_remote process.

    Note:
        abstract class"""
    __metaclass__ = abc.ABCMeta
    def _prepare_cmd(self, host, keyfile, use_sudo):
        """Prepare command regarding localhost or ssh and ssh key.

        Args:
            host (str): Ssh host name of remote host or `localhost`.
            keyfile (str): Ssh key file name. Pass `''` if no key is needed.
            use_sudo (bool): If True use ssh on remote side.

        Returns:
            List[str]
        """
        cmd = []
        if host != 'localhost':
            cmd += ['ssh']
            if keyfile:
                cmd += ['-i', keyfile]
            cmd += [host]
        if use_sudo:
            cmd += ['sudo']
        return cmd

    def _initialize_pipe(self, cmd):
        """Popen command.

        Args:
            cmd (List[str]): List of strings specifying the command to start the remote iblocksync_remote.py.
        """
        self._pipe = subprocess.Popen(cmd, bufsize=0, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)

    def fetch_bytes(self, count):
        """Read from stdout pipe

        Args:
            count (int): Number of bytes to read.

        Returns:
            str"""
        return self._pipe.stdout.read(count)

    def send_bytes(self, bytes):
        """Write bytes to stdin pipe and flush.

        Args:
            bytes (str): bytes to send"""
        self._pipe.stdin.write(bytes)
        self._pipe.stdin.flush()

    def _send_dict_as_json(self, d):
        """Send a directory packed as json.
        Args:
            d [dict]: directory"""
        self.send_bytes(json.dumps(d)+'\n')

    def _fetch_json_as_dict(self):
        """Receive a directory packed as json."""
        json_string = self._pipe.stdout.readline()
        if self._pipe.poll() is not None:
            print "Remote host accidentally died!"
            sys.exit(1)

        d = json.loads(json_string)
        return d


class Receiver(Communicator):
    """Keep communication with receiving process"""
    def __init__(self, blocksize, host, dev, keyfile, use_sudo, comment, sender):
        """Initiate Receiver communication

        Args:
            blocksize (int): Block size.
            host (str): Ssh host name of receiver host or `localhost`.
            dev (str): File name of backup file.
            keyfile (str): Ssh key file name. Pass `''` if no key is needed.
            use_sudo (bool): If True use ssh on remote side.
            comment (str): Comment that is added to the header of the backup increment.
            sender (Sender): Object of the sending Communicator.
                Use `sender.dev`_, `sender.src_blkid`_, `sender.size`.
        """
        self.dev = dev
        self._blocksize = blocksize

        """Start receiving (server_receive) process"""
        cmd = self._prepare_cmd(host, keyfile, use_sudo)
        cmd += ['python', 'iblocksync_remote.py', 'server_receive',  self.dev]

        self._initialize_pipe(cmd)

        config_data = {
            'blocksize': self._blocksize,
            'source_device_path': sender.dev,
            'source_device_blkid': sender.src_blkid,
            'comment': comment,
            'source_device_size_in_bytes': sender.size,
        }

        self._send_dict_as_json(config_data)

        meta_data = self._fetch_json_as_dict()
        if meta_data['__version__'] != __version__:
            raise VersionException('Receiver version (%s) does not match local version (%s)'
                % (meta_data['__version__'], __version__)
            )


class Sender(Communicator):
    """Keep communication with sending process"""
    def __init__(self, blocksize, host, dev, keyfile, use_sudo):
        """Initiate Sender communication

        Args:
            blocksize (int): Block size.
            host (str): Ssh host name of receiver host or `localhost`.
            dev (str): File name of source device or file.
            keyfile (str): Ssh key file name. Pass `''` if no key is needed.
            use_sudo (bool): If True use ssh on remote side.
        """
        self.dev = dev
        self._blocksize = blocksize

        """Start sending (server_send) process"""
        cmd = self._prepare_cmd(host, keyfile, use_sudo)
        cmd += ['python', 'iblocksync_remote.py', 'server_send', self.dev]

        self._initialize_pipe(cmd)

        config_data = {
            'blocksize': self._blocksize,
        }
        self._send_dict_as_json(config_data)
        meta_data = self._fetch_json_as_dict()

        if meta_data['__version__'] != __version__:
            raise VersionException('Receiver version (%s) does not match local version (%s)'
                % (meta_data['__version__'], __version__)
            )

        self.src_blkid = meta_data['src_blkid']
        self.size = meta_data['size']


class Sync(object):
    """Synchronize server_send and server_receive processes"""
    def __init__(self, blocksize, sender, receiver, pause):
        """Args:
            blocksize (int|): Block size.
            sender (Sender): Object of the sending Communicator.
            receiver (Receiver):  Object of the receiving Communicator.
            pause (int): Time to wait between each block in seconds.
            """

        self._blocksize = int(blocksize)
        print "Block size is %0.1f MiB" % (float(self._blocksize) / (1024 * 1024))

        self._sync(
            total_size=sender.size,
            pause=pause,
            sender=sender,
            receiver=receiver,
        )

    def _sync(self, total_size, pause, sender, receiver):
        """Args:
            total_size (int): Size of the source in bytes.
            pause (int): Time to wait between each block in seconds.
            sender (Sender): Object of the sending Communicator.
            receiver (Receiver):  Object of the receiving Communicator.
        """
        same_blocks = diff_blocks = 0
        is_interactive = os.isatty(sys.stdout.fileno())

        pause_ms = 0
        if pause:
            # sleep() wants seconds...
            pause_ms = float(pause) / 1000
            print "Slowing down for %d ms/block (%0.4f sec/block)" % (pause, pause_ms)


        print "Starting sync..."
        t0 = time.time()
        t_last = t0

        for offset in xrange(0, total_size, self._blocksize):
            destination_hash = receiver.fetch_bytes(LEN_HASH)
            source_hash = sender.fetch_bytes(LEN_HASH)

            if pause_ms:
                time.sleep(pause_ms)

            receiver.send_bytes(source_hash)
            sender.send_bytes(destination_hash)

            # process either blocksize or the tail
            current_block_size = min(self._blocksize, total_size - offset)

            if source_hash == destination_hash:
                same_blocks += 1
            else:
                # TODO: introduce subblocksize for more parallel send and receive
                block = sender.fetch_bytes(current_block_size)

                receiver.send_bytes(block)

                diff_blocks += 1

            t1 = time.time()

            size_read = float(offset + current_block_size)

            rate = float(size_read) / (1024.0 * 1024.0) / (t1 - t0) # in MiB/s

            time_remaining = float(total_size - size_read) / size_read * (t1 - t0) / 60 # in minutes

            if (is_interactive and ((t1 - t_last) > 1)) or (total_size == size_read):
                print "\rsame: %d, diff: %d, %6.2f %%, %5.1f MiB/s, ETR: %d min" % (same_blocks, diff_blocks, 100 * float(size_read) / total_size, rate, time_remaining),
                sys.stdout.flush()
                t_last = t1

        print # end last output of statistics

        print "\nCompleted in %d seconds" % (time.time() - t0)

        return same_blocks, diff_blocks


class VersionException(Exception):
    """Version missmatch"""


if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser(usage="%prog [options] [user@]source-host /dev/source [user@]destination-host /dev/destination")
    parser.add_option("-b", "--blocksize", dest="blocksize", type="int", help="block size (bytes, defaults to 1MiB)", default=1024 * 1024)
    parser.add_option("-c", "--comment", dest="comment", help="add an extra comment", default='')

    parser.add_option("-i", "--id", dest="keyfile", help="ssh public key file for both hosts")
    parser.add_option("--id-source", dest="keyfilesrc", help="ssh public key file for source host")
    parser.add_option("--id-destination", dest="keyfiledst", help="ssh public key file for destination host")

    parser.add_option("-p", "--pause", dest="pause", type="int", help="pause between processing blocks, reduces system load (ms, defaults to 0)")

    parser.add_option("-s", "--sudo", dest="sudo", action="store_true", help="use sudo on both hosts (defaults to off)", default=False)
    parser.add_option("--sudo-source", dest="sudosrc", action="store_true", help="use sudo on the source host (defaults to off)", default=False)
    parser.add_option("--sudo-destination", dest="sudodst", action="store_true", help="use sudo on the destination host (defaults to off)", default=False)
    (options, args) = parser.parse_args()

    if len(args) < 3:
        parser.print_help()
        print __doc__
        sys.exit(1)

    srchost = args[0]
    srcdev = args[1]
    dsthost = args[2]
    dstfile = args[3]

    if options.keyfilesrc is None:
        options.keyfilesrc = options.keyfile

    if options.keyfiledst is None:
        options.keyfiledst = options.keyfile

    if options.sudosrc is None:
        options.sudosrc = options.sudo

    if options.sudodst is None:
        options.sudodst = options.sudo

    sender = Sender(
        blocksize=options.blocksize,
        host=srchost,
        dev=srcdev,
        keyfile=options.keyfilesrc,
        use_sudo=options.sudosrc,
    )

    receiver = Receiver(
        blocksize=options.blocksize,
        host=dsthost,
        dev=dstfile,
        keyfile=options.keyfiledst,
        use_sudo=options.sudodst,
        comment=options.comment,
        sender=sender,
    )

    sync = Sync(options.blocksize, sender, receiver, options.pause)
