#!/usr/bin/env python
"""Restore incremental file image

Apply and merge incremental backups created by iblocksync_remote.py.
Run with `sudo` if root read or write privileges are required.

Combine all incremental file images up to IIMG and the original initial disk image.
Usage: restore_iimg.py [options] IIMG /dev/destination

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
import iblocksync_remote as ibs

import sys
import os

class IncrementalFileReader(ibs.IncrementalWriteableFile):
    """docstring for IncrementalFileReader"""
    def __init__(self, iimg_file_name):
        root, extension = os.path.splitext(iimg_file_name)
        try:

            if not extension.startswith('.iimg'):
                raise ValueError("IIMG filename extension must start with `.iimg'. is `%s'" % extension)
            try:
                self._backup_file_number = int(extension[5:])
            except:
                raise ValueError("IIMG filename extension must end with an integer determine the increment number.")

            dstdev = root
            self._old_backup_files = []
            for backup_file_number in xrange(self._backup_file_number+1):
                backup_increment = dstdev + '.iimg%03d' % backup_file_number # binary file difference
                #if not os.path.isfile(backup_increment):
                #   raise IOError('Backup increment `%s` does not exit.' % backup_increment)

                # backup_file_name is an existing part of the incremental backup that hast to be taken into account
                self._old_backup_files.append(ibs.BackupIncrementReader(backup_increment))
                if self._old_backup_files[-1].blocksize != self._old_backup_files[0].blocksize:
                    raise ibs.FileError("Block size (%d) in `%s' does not match expected (%d)." %
                        (self._old_backup_files[-1].blocksize, backup_file_name, self._old_backup_files[0].blocksize))

            self._old_backup_files.reverse()

            self.blocksize = self._old_backup_files[0].blocksize
            self._file = open(dstdev, 'r')

        except Exception:
            self.close()
            raise

    def re_write_current_block():
        pass


def query_yes_no(question):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}

    while True:
        sys.stderr.write(question + " [y/n] ")
        choice = raw_input().lower()
        if choice in valid:
            return valid[choice]
        else:
            sys.stderr.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")

if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser(usage="%prog [options] IIMG /dev/destination")
    parser.add_option("-f", "--force", dest="force", action="store_true", help="if destination exists, overwrite without asking.", default=False)

    (options, args) = parser.parse_args()

    if len(args) < 2:
        parser.print_help()
        print __doc__
        sys.exit(1)

    iimg_file_name = args[0]
    dst = args[1]

    incremental_file = IncrementalFileReader(iimg_file_name)

    if os.path.isfile(dst) and not options.force:
        if not query_yes_no("%s: overwrite `%s'?" % (sys.argv[0], dst)):
            sys.exit(0)

    with file(dst, 'w') as out_file:
        for block, hash_value in incremental_file:
            out_file.write(block)
