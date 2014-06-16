#!/usr/bin/env python

from __future__ import print_function

import optparse
import time

import memcacheConstants
from mc_bin_client import MemcachedClient
from mc_bin_client import MemcachedError


def check_key(src, dest, key):
    global options
    for _ in range(options.attempts):
        src_missing = dest_missing = False
        try:
            src_doc = src.getMeta(key)
        except MemcachedError as e:
            if e.status == memcacheConstants.ERR_KEY_ENOENT:
                src_missing = True
            else:
                raise

        try:
            dest_doc = dest.getMeta(key)
        except MemcachedError as e:
            if e.status == memcacheConstants.ERR_KEY_ENOENT:
                dest_missing = True
            else:
                raise

        if src_missing and dest_missing:
            # Both missing, treat as a match.
            return

        if src_doc == dest_doc:
            # Both same, match
            return

        # Differences, wait a small time for any XDCR etc to sync up before
        # next iteration
        time.sleep(0.01)
    else:
        if options.brief:
            print('x', end='', sep='')
        else:
            print("*** Differences found for '{}':".format(key))
            print(("  Source:      deleted:{} flags:{} exp:{} seqNo:{} CAS:{} ").format(
                dest_doc[0], dest_doc[1], dest_doc[2], dest_doc[3], dest_doc[4]))
            print(("  Destination: deleted:{} flags:{} exp:{} seqNo:{} CAS:{} ").format(
                src_doc[0], src_doc[1], src_doc[2], src_doc[3], src_doc[4]))


def main():
    parser = optparse.OptionParser()
    parser.add_option('-s','--source-bucket', dest="src_bucket", default="default",
                      help="source bucket to use")
    parser.add_option('-d','--dest-bucket', dest="dest_bucket", default="default",
                      help="destination bucket to use")
    parser.add_option('-v', '--verbose', action='store_true', dest='verbose',
                      help='Verbose')
    parser.add_option('-p', '--prefix', dest='prefix', default='', help='Key prefix')
    parser.add_option('-a', '--attempts', dest='attempts', default=3,
                      type='int', help='number of attempts to make to get matching metadata')
    parser.add_option('-b', '--brief', action='store_true', help='Brief output')

    global options
    options, args = parser.parse_args()

    password = ""

    if len(args) < 4:
        print("Usage: compare-meta <src_cluster> <dest_cluster> <key_min> <key_max")
        exit(1)

    src_port = dest_port = 11211
    src_name = args.pop(0)
    dest_name = args.pop(0)
    key_min = int(args.pop(0))
    key_max = int(args.pop(0))

    if ':' in src_name:
        (src_name, src_port) = src_name.split(':')
    if ':' in dest_name:
        (dest_name, dest_port) = dest_name.split(':')

    src = MemcachedClient(src_name, int(src_port))
    dest = MemcachedClient(dest_name, int(dest_port))
    src.sasl_auth_plain(options.src_bucket, password)
    dest.sasl_auth_plain(options.dest_bucket, password)

    for n in range(key_min, key_max):
        key = options.prefix + str(n)
        check_key(src, dest, key)


if __name__ == '__main__':
    main()