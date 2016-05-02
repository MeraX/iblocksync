#!/usr/bin/env python
"""
Make incremental block backups of block devices over the network

Copy this program to the remote hosts home directory.

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
import sha
LEN_HASH = 20 # length of used hash in bytes
import subprocess
import time
import struct
import json
import datetime
import string
import logging
import abc

class BlockwiseReadableFile(object):
    """Reads block-wise from a file or device

    Yields:
        string: Block of data read from file.
        string: Hash of block.


    Attributes:
        size (int): total file size in byte
    """
    def __init__(self, srcdev, blocksize):
        """Args:
            srcdev (str): Filename of source device or file.
            blocksize (int): Size of blocks to read and hash in bytes.
        """
        try:
            self._file = open(srcdev, 'r')

            self.blocksize = blocksize

            # get total size
            self._file.seek(0, 2)
            self.size = self._file.tell()
            self._file.seek(0)

        except:
            self.close()
            raise

    def __enter__(self):
        """__enter__ and __exit for 'with S() as s:' syntax"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """__enter__ and __exit for 'with S() as s:' syntax"""
        return self.close()

    def close(self):
        if hasattr(self,'_file'):
            self._file.close()

    def __iter__(self):
        return self

    def next(self):
        """Make self iterable (python 2.7 style)."""
        block, hash_value = self._read_block()
        if len(block) > 0:
            return block, hash_value
        else:
            logging.debug('do not actually use last read_block')
            raise StopIteration

    def _read_block(self):
        """Read one block and calculate its hash.

        Returns:
            string: Block of data read from file.
            string: Hash of block."""
        block = self._file.read(self.blocksize)
        hash_value = sha.sha(block).digest()

        return block, hash_value


class IncrementalWriteableFile(BlockwiseReadableFile):
    """Read initial backup image and existing incremental images.
    Write new blocks in a separable file named `name+'.iimg###'`_.

    ### is the number of the incremental image ranging from 000 for the
    fist increment to a maximum of 999.

    uses proprietary file format:
    First row (ending with first `\n') contains a human readable header. Json
    Format is used. Entry 'block_size' giving the block size in byte (integer)
    is mandatory.
    The following data is raw binary data than can be divided in blocks.
    Each block consists of a block header (28 bytes) and a data block:
        * 8 bytes representing a 64 bit unsigned int little-endian (`<Q' format)
        * 20 (LEN_HASH) bytes with the sha1 hash of the block
        * N bytes of the actual data. N is block size expect for the last block.
          It may be smaller if the device size is not divisible by the block
          size without remainder
    """
    def __init__(self, srcdev, initial_backup, blocksize, comment, src_blkid, src_size):
        """Opens initial backup file and existing backup increments.
        Write header into new backup increment.

        Args:
            srcdev (str): Filename of source device or file.
            initial_backup (str): Filename of backup file.
            blocksize (int): Size of blocks to read and hash in bytes.
            comment (str): Comment that is added to the header of the backup increment.
            src_blkid (str): 'Blkid of the source device.'.
            src_size (int): Size of the source device or file in bytes.

        """
        if len(sha.sha('').digest()) != LEN_HASH:
            raise ValueError('Hash function returned unexpected hash.')

        try:
            super(IncrementalWriteableFile, self).__init__(initial_backup, blocksize)

            self._old_backup_files = []
            for backup_file_number in xrange(1000):
                backup_file_name = initial_backup + '.iimg%03d' % backup_file_number # binary file difference
                if not os.path.isfile(backup_file_name):
                    break

                # backup_file_name is an existing part of the incremental backup that hast to be  taken into account
                self._old_backup_files.append(BackupIncrementReader(backup_file_name))
                if not self._old_backup_files[-1].blocksize == blocksize:
                    raise FileError("Block size (%d) in `%s' does not match expected (%d)." %
                        (self._old_backup_files[-1].blocksize, backup_file_name, blocksize))
            else:
                raise ValueError('Can not make more than %d incremental backups.' % backup_file_number + 1)

            # reverse order such that latest file comes first
            self._old_backup_files.reverse()

            # file object of the new difference file
            self._incremental_img = open(backup_file_name, 'w')
            self._incremental_img.write(json.dumps({
                'block_size': self.blocksize,
                'local_date_of_backup': str(datetime.datetime.now()), #TODO: improve, include time zone
                'source_device_path': srcdev,
                'source_device_blkid': src_blkid,
                'comment': comment,
                'source_device_size_in_bytes': src_size,
                'iblocksync__version__':__version__,
                'file_format': '<this JSON header> \\n [ <8 byte block ID (unsigned long long little-endian)> <20 bytes block SHA-1 hash> <block_size bytes block of data> ]...',
                })+'\n')

        except Exception:
            self.close()
            raise

    def close(self):
        super(IncrementalWriteableFile, self).close()
        if hasattr(self, '_old_backup_files'):
            for f in self._old_backup_files:
                f.close()
        if hasattr(self, '_incremental_img'):
            self._incremental_img.close()

    def _read_block(self):
        """Read one block and its hash.
        If the block exists in any backup increment use the latest one.
        Else read from the initial backup file and calculate its blocks hash.

        Returns:
            string: Block of data read from file.
            string: Hash of block."""
        current_offset = self._file.tell()
        block = None
        # iterate tough all increments starting with the latest one
        for old_backup_f in self._old_backup_files:
            if old_backup_f.offset == current_offset:
                if block is None:
                    logging.debug('use block %i from %s' % (current_offset, old_backup_f._file))
                    block, hash_value = old_backup_f.read_block()
                else:
                    # we already have the latest block for the current offset
                    logging.debug('do not use block %i from %s' % (current_offset, old_backup_f._file))
                    old_backup_f.skip_block()

        if block is None:
            logging.debug('use block %i from destination device' % (current_offset))
            return super(IncrementalWriteableFile, self)._read_block()
        else:
            # every block except last block should len(block) == self.blocksize
            # seeking more than len(block) with the last block is okay.
            self._file.seek(self.blocksize, 1)
            logging.debug('seek %i. len(block) = %i. tell() = %i' %(self.blocksize, len(block), self._file.tell()))
            return block, hash_value

    def re_write_current_block(self, newblock, hash_value):
        """Write a new block and its hash to the new backup increment.

        Args:
            newblock (str): new block of data.
            hash_value (str): hash of the new block
        """
        assert len(newblock) == self.blocksize
        assert len(hash_value) == LEN_HASH

        block_header = struct.pack('<Q', self._file.tell()-len(newblock))
        self._incremental_img.write(block_header + hash_value)
        self._incremental_img.write(newblock)


class ParsingError(Exception):
    """Could not parse."""


class FileError(Exception):
    """This is not the increment you are looking for."""


class BackupIncrementReader(object):
    """Read a backup increment."""
    def __init__(self, name):
        """Args:
            name (str): File name of the increment.
        """
        self._file = open(name, 'r')

        self._read_header()
        self._read_block_header()

    def _read_header(self):
        """Read self.blocksize file from header."""
        header = self._file.readline()
        try:
            header_dict = json.loads(header)
            self.blocksize = header_dict['block_size']
        except:
            header = sanitize_string(header, 300)
            raise ParsingError(
                "Could not parse header (%s) of '%s' (JSON error)"
                % (header, self._file.name)
            )

        if self.blocksize <= 0:
            header = sanitize_string(header, 300)
            raise ParsingError("Negative blocksize %s in '%s'"
                % (self.blocksize, self._file.name) )

    def _read_block_header(self):
        """ read self.offset from header."""
        block_header = self._file.read(8 + LEN_HASH)
        if block_header == '':
            return self.close()
        assert len(block_header) == 8 + LEN_HASH
        self.offset, = struct.unpack('<Q', block_header[:8])
        self.hash_value = block_header[8:]

    def read_block(self):
        """Read and return current block. Read next header of next block.

        Returns:
            string: Block of data read from file.
            string: Hash of block.
        """
        block = self._file.read(self.blocksize)
        hash_value = self.hash_value
        self._read_block_header()
        return block, hash_value

    def skip_block(self):
        """Skip current block."""
        if self._file.closed:
            return
        self._file.seek(self.blocksize, 1)
        self._read_block_header()

    def close(self):
        self._file.close()
        self.offset = -1
        self.hash_value = ''


def sanitize_string(byte_string, max_length):
    """Remove all non ascii chars and trim to maximum length max_length.

    Args:
        byte_string (str): Any string containing ascii and non ascii chars.
        max_length (int): Maximum length of returned string.
    Returns:
        str
    """
    byte_string = filter(lambda c: c in string.printable, byte_string)
    max_length = max(3,max_length)
    if len(byte_string) <= max_length:
        return byte_string
    return byte_string[:max_length-3]+'...'


def server_receive(initial_backup):
    """Server function for receiving data and writing incremental backups.

    Args:
        initial_backup (str): Filename of backup file.
    """
    config_data = sys.stdin.readline()
    config_data = json.loads(config_data)
    blocksize = config_data['blocksize']
    srcdev = config_data['source_device_path']
    src_blkid = config_data['source_device_blkid']
    comment = config_data['comment']
    src_size = config_data['source_device_size_in_bytes']

    meta_data = {
        '__version__': __version__,
    }
    print json.dumps(meta_data)
    sys.stdout.flush()

    with IncrementalWriteableFile(srcdev, initial_backup, blocksize, comment, src_blkid, src_size) as incremental_file:
        bytes_compared = 0
        for block, hash_value in incremental_file:
            destination_hash = hash_value
            sys.stdout.write(destination_hash)
            sys.stdout.flush()
            source_hash = sys.stdin.read(LEN_HASH)
            if source_hash != destination_hash:
                current_block_size = min(blocksize, src_size - bytes_compared)
                newblock = sys.stdin.read(current_block_size)
                incremental_file.re_write_current_block(newblock, source_hash)

            bytes_compared += blocksize


def server_send(srcdev):
    """Server function for sending data.

    Args:
        srcdev (str): Filename of source device or file.
    """
    config_data = sys.stdin.readline()
    config_data = json.loads(config_data)
    blocksize = config_data['blocksize']

    # try to get blkid from source device
    try:
        p = subprocess.Popen(['blkid', srcdev], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        src_blkid, stderr = p.communicate()
    except:
        src_blkid = ''

    with BlockwiseReadableFile(srcdev, blocksize) as source_file:
        meta_data = {
            'src_blkid' : src_blkid,
            'size': source_file.size,
            '__version__': __version__,
        }
        print json.dumps(meta_data)
        sys.stdout.flush()

        for block, hash_value in source_file:
            source_hash = hash_value
            sys.stdout.write(source_hash)
            sys.stdout.flush()
            destination_hash = sys.stdin.read(LEN_HASH)
            if destination_hash != source_hash:
                sys.stdout.write(block)
                sys.stdout.flush()


if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser(usage="%prog [server_receive|server_send] device")
    (options, args) = parser.parse_args()

    if len(args) != 2:
        parser.print_help()
        print __doc__
        sys.exit(1)

    debug_level = logging.WARNING
    # uncomment do log debug messages
    #debug_level = logging.DEBUG
    if args[0] == 'server_receive':
        logging.basicConfig(filename='iblocksync_receive.log',level=debug_level)
        logging.debug('start')
        initial_backup = args[1]
        server_receive(initial_backup)
    elif args[0] == 'server_send':
        logging.basicConfig(filename='iblocksync_send.log',level=debug_level)
        logging.debug('start')
        srcdev = args[1]
        server_send(srcdev)
    else:
        parser.print_help()
        print __doc__
        sys.exit(1)

    logging.shutdown()
    if os.stat('iblocksync_receive.log').st_size == 0 :
        os.remove('iblocksync_receive.log')
    if os.stat('iblocksync_send.log').st_size == 0 :
        os.remove('iblocksync_send.log')
