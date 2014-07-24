#!/usr/bin/env python

# Tool which synchonises a given document between two clusters, including metadata.
# Given references to two clusters, if the document is *only* found on one
# of the two it is copied to the other.

# If it exists on both clusters, then it will refuse to copy them.

from __future__ import print_function

import memcacheConstants
from mc_bin_client import MemcachedClient
from mc_bin_client import MemcachedError

import sys
import optparse


class Document:
    def __init__(self, fields):
        self.fields = fields
    def __eq__(self, other):
        return self.fields == other.fields
    def deleted(self):
        return self.fields[0]
    def flags(self):
        return self.fields[1]
    def exp(self):
        return self.fields[2]
    def seqno(self):
        return self.fields[3]
    def cas(self):
        return self.fields[4]
    def value(self):
        return self.fields[5]


def get_matching_meta(cluster, key, attempts):
    """Attempt to get a matching value and metadata for a key (using get() and
    getMeta(). Will retry up to attempts times. Returns a tuple of the fields
    on success, else None."""
    try:
        for _ in range(attempts):
                (deleted, flags, exp, seqno, meta_cas) = cluster.getMeta(key)
                if deleted:
                    value = ""
                    break
                (_, cas, value) = cluster.get(key)
                if cas == meta_cas:
                    break
        else:
            # Failed
            return ("EINCONSISTANT", None)
        return (None, Document((deleted, flags, exp, seqno, meta_cas, value)))
    except MemcachedError as e:
        if e.status == memcacheConstants.ERR_KEY_ENOENT:
            return ("ENOENT", None)
        else:
            raise


def print_doc(title, d):
    if d.deleted() == 1:
        value = "DELETED"
    else:
        value = d.value()
    print(("  {0:25} : deleted:{1} flags:{2} exp:{3} seqNo:{4} CAS:{5} " +
           "value:{6}...").format(title, d.deleted(), d.flags(), d.exp(),
                                  d.seqno(), d.cas(), value[:30]))


def docs_equal(src_err, src_doc, dest_err, dest_doc):
    """Returns true if the given src & dest docs should be considered equal."""
    if not src_err:
        return src_doc == dest_doc
    else:
        return src_err == "ENOENT" and dest_doc.deleted() == 1


def synchronize_key(src, dest, key):
    """Reads a document+metadata from the source; then attempts to set the same
    doc+meta on the destination."""

    global options

    (src_err, src_doc) = get_matching_meta(src, key, 3)
    if src_err:
        if src_err == "EINCONSISTANT":
            print(("  Error: failed to get consistant data & metadata from " +
                   "source - skipping.").format(key), file=sys.stderr)
            return
        elif src_err == "ENOENT":
            if not options.delete_if_missing:
                print(("  Error: no such key '{}' on souce - skipping. If " +
                       "you want to delete this from destination, run with " +
                       "--delete-if-missing").format(key), file=sys.stderr)
                return
        else:
            raise

    if (not src_doc) and options.verbose:
            print("  Source               : missing")

    (dest_err, dest_doc) = get_matching_meta(dest, key, 3)
    if not dest_err:

        if docs_equal(src_err, src_doc, dest_err, dest_doc):
            if options.verbose:
                print(("Key: {}  Source and Destination match - " +
                      "skipping.").format(key))
            return

    # We've identified that there's a difference to resolve.
    # Print the key and begin the resolution code.
    print("Key: {}".format(key))

    if dest_doc and options.overwrite:
        print_doc("Dest before sync", dest_doc)

        if src_doc:
            print_doc("Source", src_doc)
            changed_source_document=False
            # Check revIDs are increasing.
            if dest_doc.seqno() >= src_doc.seqno():
                if options.allow_src_changes:
                    # We are allowed to change source, so fix this by bumping
                    # up the source's to dest_revID+1.
                    src.setWithMeta(key, src_doc.value(), src_doc.exp(),
                                    src_doc.flags(), dest_doc.seqno() + 1,
                                    src_doc.cas(), src_doc.cas())
                    # Refetch CAS, etc from new document.
                    (src_err, src_doc) = get_matching_meta(src, key, 3)
                    if not src_doc:
                        print(("Error: failed to get consistent data & " +
                               "metadata from source - skipping.")
                               .format(key), file=sys.stderr)
                        return

                    print_doc("Source after revID fix", src_doc)
                    changed_source_document=True
                    print(("  Resolution - Changed on source."))
                else:
                    print(("Error: Destination revID '{}' greater than " +
                           "source revID '{}'. Cannot synchronize " +
                           "unless --allow-source-changes is enabled.")
                           .format(dest_doc.seqno(), src_doc.seqno()),
                           file=sys.stderr)
                    return

            # If XDCR isn't enabled and we didn't change the source, try and change dest.
            if not options.xdcr_enabled and not changed_source_document:
                try:
                     dest.setWithMeta(key, src_doc.value(), src_doc.exp(),
                                      src_doc.flags(), src_doc.seqno(),
                                      src_doc.cas(), dest_doc.cas())

                except MemcachedError as e:
                    if e.status == memcacheConstants.ERR_KEY_EEXISTS:
                        print("Error: Got EEXISTS during setWithMeta(). " +
                              "Possible CAS mismatch setting at " +
                              "destination.", file=sys.stderr)
                print(("  Resolution - Changed on dest."))

        else: # No source document - just delete destination.
            print(("  Resolution - Deleting from destination."))
            dest.delete(key)

    else:
        if src_doc:
            print_doc("Source", src_doc)
            # Doesn't exist yet - use addWithMeta.
            try:
                dest.addWithMeta(key, src_doc.value(), src_doc.exp(),
                             src_doc.flags(), src_doc.seqno(), src_doc.cas())
            except MemcachedError as e:
                if e.status == memcacheConstants.ERR_KEY_EEXISTS:
                    print(("Error: key '{}' already exists on destination " +
                           "cluster. Run with --overwrite to " +
                           "overwrite.").format(key), file=sys.stderr)
                else:
                    raise
            print("  Resolution - added to destination.")
        else:
            # No source or destination doc - nothing to do.
            print(("Error: key '{}' doesn't exist on either source or " +
                   "destination - ignoring.").format(key), file=sys.stderr)
            return

    if options.validate:
        # Fetch to double-check it matches:
        (dest_err, dest_doc) = get_matching_meta(dest, key, 3)

        same = docs_equal(src_err, src_doc, dest_err, dest_doc)       
        if same:
            print("  OK")
        else:
            if options.xdcr_enabled:
                level="WARNING"
            else:
                level="ERROR"

            print(("{}: key '{}' Src & dest differ *after* setWithMeta :")
                  .format(level, key), file=sys.stderr)

        if not same:
            print_doc("Dest after sync", dest_doc)


def main(args):
    usage = "usage: %prog [options] source-cluster-IP dest-cluster-IP key1 key2 ... keyn"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-s','--source-bucket', dest="src_bucket",
                      default="default",
                      help="source bucket to use")

    parser.add_option('-d','--dest-bucket', dest="dest_bucket",
                      default="default",
                      help="destination bucket to use")

    parser.add_option('-o', '--overwrite', action='store_true',
                      dest='overwrite',
                      help='Overwrite destination document if it already ' +
                           'exists.')

    parser.add_option('-a', '--allow-source-changes', action='store_true',
                      dest='allow_src_changes',
                      help=('Allow changes to the source metadata ' +
                            '(e.g. revID) to be made. Necessary ' +
                            'to synchronize documents.'))

    parser.add_option('-D', '--delete-if-missing', action='store_true',
                       dest='delete_if_missing', help='Delete document from ' +
                            'destination if it doesn\'t exist (and no ' +
                            'tombstone present) on the source.')

    parser.add_option('-v', '--verbose', action='store_true', dest='verbose',
                      help='Verbose')

    parser.add_option('-x', '--xdcr_enabled', action='store_true',
                      dest='xdcr_enabled',
                      help='Set if XDCR is enabled for the specified ' +
                           'bucket (between the clusters).')

    parser.add_option('-V', '--validate', action='store_true', dest='validate',
                      help='Validate the destination matches the source ' +
                           'after a sync. Note that this adds an extra ' +
                           'get/get_meta to destination and can race with ' +
                           'XDCR for false positives.')

    global options
    options, args = parser.parse_args()

    password = ""

    if len(args) < 3:
        parser.print_help()
        exit(1)

    src_port = dest_port = 11211
    src_name = args.pop(0)
    dest_name = args.pop(0)
    if ':' in src_name:
        (src_name, src_port) = src_name.split(':')
    if ':' in dest_name:
        (dest_name, dest_port) = dest_name.split(':')

    src = MemcachedClient(src_name, int(src_port))
    dest = MemcachedClient(dest_name, int(dest_port))
    src.sasl_auth_plain(options.src_bucket, password)
    dest.sasl_auth_plain(options.dest_bucket, password)

    for key in args:
        synchronize_key(src, dest, key)

if __name__ == '__main__':
    sys.exit(main(sys.argv))
