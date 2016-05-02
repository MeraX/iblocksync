#!/bin/bash

function error {
	echo $@ >&2
	exit 1
}

function assert_empy_iimg {
	if [[ $(cat "$1" | wc -l) != '1' ]]; then
		error "\`$1' should only contain one line (json)."
	fi
}

if [[ -e test ]]; then
	error "file or directory \`test' must not exist before running test.sh."
fi

mkdir test

# create random 7.2MB file
dd if=/dev/urandom bs=100000 count=72 of=test/source #

# test from local to local
./iblocksync.py localhost test/source localhost test/destination
if [[ ! $? ]]; then
	error "test/destination does not exist -> assert error."
fi

# make copy as base backup img
cp test/source test/destination

# make first incremental backup (should be empty.)
./iblocksync.py --blocksize 1048576 localhost test/source localhost test/destination
RETURN=$?
if [[ "$RETURN" != "0" ]]; then
	error "simple iblocksync failed. return: $RETURN"
fi
if [[ ! -f "test/destination.iimg000" ]]; then
	error "simple iblocksync failed. \`test/destination.iimg000' was not created."
fi

assert_empy_iimg test/destination.iimg000

# make second increment
./iblocksync.py -b 1048576 localhost test/source localhost test/destination
assert_empy_iimg test/destination.iimg001

# vary one 1MB block
dd if=/dev/urandom bs=100000 count=7 seek=21 conv=notrunc of=test/source

# only one block should be changes
./iblocksync.py --blocksize=1048576 localhost test/source localhost test/destination
if [[ $(stat -c%s test/destination.iimg002) -le $(stat -c%s test/destination.iimg001) ]]; then
	error "something should be changed."
fi

# vary two 1MB blocks
dd if=/dev/urandom bs=100000 count=7 seek=15 conv=notrunc of=test/source

./iblocksync.py --blocksize=1048576 localhost test/source localhost test/destination
if [[ $(stat -c%s test/destination.iimg003) -le $(stat -c%s test/destination.iimg002) ]]; then
	error "Two blocks are changed. Image 003 should be larger than 002."
fi

# restore backup
./iblocksync_restore.py test/destination.iimg003 test/source_restored003
RETURN=$?
if [[ "$RETURN" != "0" ]]; then
	error "simple iblocksync_restore failed. return: $RETURN"
fi

diff -q test/source test/source_restored003
RETURN=$?
if [[ "$RETURN" != "0" ]]; then
	error "\`test/source' and \`test/source_restored003' should be the same. diff return: $RETURN"
fi

# restore backup
./iblocksync_restore.py test/destination.iimg002 test/source_restored002
diff -q test/source test/source_restored002
RETURN=$?
if [[ "$RETURN" == "0" ]]; then
	error "\`test/source' and \`test/source_restored002' should be different. diff return: $RETURN"
fi

# create a 4th, empty backup increment
./iblocksync.py --blocksize=1048576 localhost test/source localhost test/destination

./iblocksync_restore.py test/destination.iimg004 test/source_restored004
diff -q test/source_restored003 test/source_restored004
RETURN=$?
if [[ "$RETURN" != "0" ]]; then
	error "\`test/source_restored003' and \`test/source_restored004' should be the same. diff return: $RETURN"
fi

rm -r test

echo "All tests ended successfully."
