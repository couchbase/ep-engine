/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef STORED_VALUE_H
#define STORED_VALUE_H 1

#include <climits>
#include <cstring>
#include <algorithm>

#include "common.hh"
#include "item.hh"
#include "locks.hh"
#include "stats.hh"
#include "histo.hh"
#include "queueditem.hh"

extern "C" {
    extern rel_time_t (*ep_current_time)();
}

// Forward declaration for StoredValue
class HashTable;
class StoredValueFactory;

// One of the following structs overlays at the end of StoredItem.
// This is figured out dynamically and stored in one bit in
// StoredValue so it can figure it out at runtime.

/**
 * StoredValue "small" data storage.
 */
struct small_data {
    uint8_t keylen;             //!< Length of the key.
    char    keybytes[1];        //!< The key itself.
};

/**
 * StoredValue "featured" data type.
 */
struct feature_data {
    uint64_t   cas;             //!< CAS identifier.
    time_t     exptime;         //!< Expiration time of this item.
    rel_time_t lock_expiry;     //!< getl lock expiration
    bool       locked : 1;      //!< True if this item is locked
    bool       resident : 1;    //!< True if this object's value is in memory.
    uint8_t    keylen;          //!< Length of the key
    char       keybytes[1];     //!< The key itself.
};

/**
 * Union of StoredValue data.
 */
union stored_value_bodies {
    struct small_data   small;  //!< The small type.
    struct feature_data feature; //!< The featured type.
};

/**
 * Contents stored when swapped out.
 */
union blobval {
    uint32_t len;               //!< The length as an integer.
    char     chlen[4];          //!< The length as a four byte integer
};

/**
 * In-memory storage for an item.
 */
class StoredValue {
public:

    void operator delete(void* p) {
        ::operator delete(p);
     }

    /**
     * Mark this item as needing to be persisted.
     */
    void markDirty() {
        dirtiness = ep_current_time() >> 2;
        _isDirty = 1;
    }

    /**
     * Mark this item as dirty as of a certain time.
     *
     * This method is primarily used to mark an item as dirty again
     * after a storage failure.
     *
     * @param dataAge the previous dataAge of this record
     */
    void reDirty(rel_time_t dataAge) {
        dirtiness = dataAge >> 2;
        _isDirty = 1;
        clearPendingId();
    }

    // returns time this object was dirtied.
    /**
     * Mark this item as clean.
     *
     * @param dataAge an output parameter that captures the time this
     *                item was marked dirty
     */
    void markClean(rel_time_t *dataAge) {
        if (dataAge) {
            *dataAge = dirtiness << 2;
        }
        _isDirty = 0;
    }

    /**
     * True if this object is dirty.
     */
    bool isDirty() const {
        return _isDirty;
    }

    /**
     * True if this object is not dirty.
     */
    bool isClean() const {
        return !isDirty();
    }

    /**
     * Check if this item is expired or not.
     *
     * @param asOf the time to be compared with this item's expiry time
     * @return true if this item's expiry time < asOf
     */
    bool isExpired(time_t asOf) const {
        if (getExptime() != 0 && getExptime() < asOf) {
            return true;
        }
        return false;
    }

    /**
     * Get the pointer to the beginning of the key.
     */
    const char* getKeyBytes() const {
        if (_isSmall) {
            return extra.small.keybytes;
        } else {
            return extra.feature.keybytes;
        }
    }

    /**
     * Get the length of the key.
     */
    uint8_t getKeyLen() const {
        if (_isSmall) {
            return extra.small.keylen;
        } else {
            return extra.feature.keylen;
        }
    }

    /**
     * True of this item is for the given key.
     *
     * @param k the key we're checking
     * @return true if this item's key is equal to k
     */
    bool hasKey(const std::string &k) const {
        return k.length() == getKeyLen()
            && (std::memcmp(k.data(), getKeyBytes(), getKeyLen()) == 0);
    }

    /**
     * Get this item's key.
     */
    const std::string getKey() const {
        return std::string(getKeyBytes(), getKeyLen());
    }

    /**
     * Get this item's value.
     */
    value_t getValue() const {
        return value;
    }

    /**
     * Get the expiration time of this item.
     *
     * @return the expiration time for feature items, 0 for small items
     */
    time_t getExptime() const {
        if (_isSmall) {
            return 0;
        } else {
            return extra.feature.exptime;
        }
    }

    void setExptime(time_t tim) {
        if (!_isSmall) {
            extra.feature.exptime = tim;
            markDirty();
        }
    }

    /**
     * Get the client-defined flags of this item.
     *
     * @return the flags for feature items, 0 for small items
     */
    uint32_t getFlags() const {
        return flags;
    }

    /**
     * Get the number of times this value was replicated.
     *
     * @return the number of times this value was replicaded
     */
    uint8_t getNumReplicas() const {
        return replicas;
    }

    void incrementNumReplicas(uint8_t count = 1) {
        replicas += count;
    }

    /**
     * Set a new value for this item.
     *
     * @param v the new value
     * @param newFlags the new client-defined flags
     * @param newExp the new expiration
     * @param theCas thenew CAS identifier
     * @param stats the global stats
     * @param ht the hashtable that contains this StoredValue instance
     */
    void setValue(value_t v, uint32_t newFlags, time_t newExp,
                  uint64_t theCas, EPStats &stats, HashTable &ht) {
        reduceCurrentSize(stats, ht, size());
        value = v;
        setResident();
        flags = newFlags;
        if (!_isSmall) {
            extra.feature.cas = theCas;
            extra.feature.exptime = newExp;
        }
        markDirty();
        increaseCurrentSize(stats, ht, size());
        replicas = 0;
    }

    size_t valLength() {
        if (isDeleted()) {
            return 0;
        } else if (isResident()) {
            return value->length();
        } else {
            blobval uval;
            assert(value->length() == sizeof(uval));
            std::memcpy(uval.chlen, value->getData(), sizeof(uval));
            return static_cast<size_t>(uval.len);
        }
    }

    size_t getKeyValLength() {
        return valLength() + getKeyLen();
    }

    /**
     * Eject an item value from memory.
     * @param stats the global stat instance
     * @param ht the hashtable that contains this StoredValue instance
     */
    bool ejectValue(EPStats &stats, HashTable &ht);

    /**
     * Restore the value for this item.
     * @param v the new value to be restored
     * @param stats the global stat instance
     * @param ht the hashtable that contains this StoredValue instance
     */
    bool restoreValue(value_t v, EPStats &stats, HashTable &ht);

    /**
     * Get this item's CAS identifier.
     *
     * @return the cas ID for feature items, 0 for small items
     */
    uint64_t getCas() const {
        if (_isSmall) {
            return 0;
        } else {
            return extra.feature.cas;
        }
    }

    /**
     * Get the time of dirtiness of this item.
     *
     * Note that the clock loses four bits of resolution, so the
     * timestamp only has four seconds of accuracy.
     */
    rel_time_t getDataAge() const {
        return dirtiness << 2;
    }

    /**
     * Set a new CAS ID.
     *
     * This is a NOOP for small item types.
     */
    void setCas(uint64_t c) {
        if (!_isSmall) {
            extra.feature.cas = c;
        }
    }

    /**
     * Lock this item until the given time.
     *
     * This is a NOOP for small item types.
     */
    void lock(rel_time_t expiry) {
        if (!_isSmall) {
            extra.feature.locked = true;
            extra.feature.lock_expiry = expiry;
        }
    }

    /**
     * Unlock this item.
     */
    void unlock() {
        if (!_isSmall) {
            extra.feature.locked = false;
            extra.feature.lock_expiry = 0;
        }
    }

    /**
     * True if this item has an ID.
     *
     * An item always has an ID after it's been persisted.
     */
    bool hasId() {
        return id > 0;
    }

    /**
     * Get this item's ID.
     *
     * @return the ID for the item; 0 if the item has no ID
     */
    int64_t getId() {
        return id;
    }

    /**
     * Set the ID for this item.
     *
     * This is used by the persistene layer.
     *
     * It is an error to set an ID on an item that already has one.
     */
    void setId(int64_t to) {
        assert(!hasId());
        id = to;
        assert(hasId());
    }

    /**
     * Clear the ID (after disk deletion when an object was reused).
     */
    void clearId() {
        id = -1;
        assert(!hasId());
    }

    /**
     * Is this item currently waiting to receive a new ID?
     *
     * This is the case when it's been submitted to the storage layer
     * and has been marked clean, but has not yet received its ID.
     *
     * @return true if the item is waiting for an ID.
     */
    bool isPendingId() {
        return id == -2;
    }

    /**
     * Set this item to be pending an ID.
     */
    void setPendingId() {
        assert(!hasId());
        assert(!isPendingId());
        id = -2;
    }

    /**
     * If we're still in a pending ID state, clear the state.
     */
    void clearPendingId() {
        if (isPendingId()) {
            id = -1;
        }
    }

    /**
     * Get the total size of this item.
     *
     * @return the amount of memory used by this item.
     */
    size_t size() {
        // This differs from valLength in that it reports the
        // *resident* length instead of the length of the actual value
        // as it existed.
        size_t vallen = isDeleted() ? 0 : value->length();
        size_t valign = std::min(sizeof(void*),
                                 sizeof(void*) - vallen % sizeof(void*));
        size_t kalign = std::min(sizeof(void*),
                                 sizeof(void*) - getKeyLen() % sizeof(void*));

        return sizeOf(_isSmall) + getKeyLen() + vallen +
            sizeof(value_t) + valign + kalign;
    }

    /**
     * Return true if this item is locked as of the given timestamp.
     *
     * @param curtime lock expiration marker (usually the current time)
     * @return true if the item is locked
     */
    bool isLocked(rel_time_t curtime) {
        if (_isSmall) {
            return false;
        } else {
            if (extra.feature.locked && (curtime > extra.feature.lock_expiry)) {
                extra.feature.locked = false;
                return false;
            }
            return extra.feature.locked;
        }
    }

    /**
     * True if this value is resident in memory currently.
     */
    bool isResident() {
        if (_isSmall) {
            return true;
        } else {
            return extra.feature.resident;
        }
    }

    /**
     * True if this object is logically deleted.
     */
    bool isDeleted() const {
        return value.get() == NULL;
    }

    /**
     * Logically delete this object.
     */
    void del(EPStats &stats, HashTable &ht) {
        size_t oldsize = size();

        value.reset();
        markDirty();
        setCas(getCas() + 1);

        size_t newsize = size();
        if (oldsize < newsize) {
            increaseCurrentSize(stats, ht, newsize - oldsize, true);
        } else if (newsize < oldsize) {
            reduceCurrentSize(stats, ht, oldsize - newsize, true);
        }
    }

    /**
     * Get the size of a StoredValue object.
     *
     * This method exists because the size of a StoredValue as used
     * cannot be determined entirely at compile time due to the two
     * different extras sections that are used.
     *
     * @param small if true, we want the small variety, otherwise featured
     *
     * @return the size in bytes required (minus key) for a StoredValue.
     */
    static size_t sizeOf(bool small) {
        // Subtract one because the length of the string is computed on demand.
        size_t base = sizeof(StoredValue) - sizeof(union stored_value_bodies) - 1;
        return base + (small ? sizeof(struct small_data) : sizeof(struct feature_data));
    }

    /**
     * Set the maximum amount of data this instance can store.
     *
     * While there's other overhead, this only takes into
     * consideration the sum of StoredValue::size() values.
     */
    static void setMaxDataSize(EPStats&, size_t);

    /**
     * Get the maximum amount of memory this instance can store.
     */
    static size_t getMaxDataSize(EPStats&);

    /**
     * Get the current amount of of data stored.
     */
    static size_t getCurrentSize(EPStats&);

private:

    StoredValue(const Item &itm, StoredValue *n, EPStats &stats, HashTable &ht,
                bool setDirty = true, bool small = false) :
        value(itm.getValue()), next(n), id(itm.getId()),
        dirtiness(0), _isSmall(small), flags(itm.getFlags()), replicas(0)
    {

        if (_isSmall) {
            extra.small.keylen = itm.getKey().length();
        } else {
            extra.feature.cas = itm.getCas();
            extra.feature.exptime = itm.getExptime();
            extra.feature.locked = false;
            extra.feature.resident = true;
            extra.feature.lock_expiry = 0;
            extra.feature.keylen = itm.getKey().length();
        }

        if (setDirty) {
            markDirty();
        } else {
            markClean(NULL);
        }

        increaseCurrentSize(stats, ht, size());
    }

    void setResident() {
        if (!_isSmall) {
            extra.feature.resident = 1;
        }
    }

    friend class HashTable;
    friend class StoredValueFactory;

    value_t            value;          // 16 bytes
    StoredValue        *next;          // 8 bytes
    int64_t            id;             // 8 bytes
    uint32_t           dirtiness : 30; // 30 bits -+
    bool               _isSmall  :  1; // 1 bit    | 4 bytes
    bool               _isDirty  :  1; // 1 bit  --+
    uint32_t           flags;          // 4 bytes
    Atomic<uint8_t>    replicas;       // 1 byte


    union stored_value_bodies extra;

    static void increaseCurrentSize(EPStats&, HashTable &ht,
                                    size_t by, bool residentOnly = false);
    static void reduceCurrentSize(EPStats&, HashTable &ht,
                                  size_t by, bool residentOnly = false);
    static bool hasAvailableSpace(EPStats&, const Item &item);

    DISALLOW_COPY_AND_ASSIGN(StoredValue);
};

/**
 * Mutation types as returned by store commands.
 */
typedef enum {
    /**
     * Storage was attempted on a vbucket not managed by this node.
     */
    INVALID_VBUCKET,
    NOT_FOUND,                  //!< The item was not found for update
    INVALID_CAS,                //!< The wrong CAS identifier was sent for a CAS update
    WAS_CLEAN,                  //!< The item was clean before this mutation
    WAS_DIRTY,                  //!< This item was already dirty before this mutation
    IS_LOCKED,                  //!< The item is locked and can't be updated.
    NOMEM                       //!< Insufficient memory to store this item.
} mutation_type_t;

/**
 * Result from add operation.
 */
typedef enum {
    ADD_SUCCESS,                //!< Add was successful.
    ADD_NOMEM,                  //!< No memory for operation
    ADD_EXISTS,                 //!< Did not update -- item exists with this key
    ADD_UNDEL                   //!< Undeletes an existing dirty item
} add_type_t;

/**
 * Base class for visiting a hash table.
 */
class HashTableVisitor {
public:
    virtual ~HashTableVisitor() {}

    /**
     * Visit an individual item within a hash table.
     *
     * @param v a pointer to a value in the hash table
     */
    virtual void visit(StoredValue *v) = 0;
    /**
     * True if the visiting should continue.
     *
     * This is called periodically to ensure the visitor still wants
     * to visit items.
     */
    virtual bool shouldContinue() { return true; }
};

/**
 * Hash table visitor that reports the depth of each hashtable bucket.
 */
class HashTableDepthVisitor {
public:
    virtual ~HashTableDepthVisitor() {}

    /**
     * Called once for each hashtable bucket with its depth.
     *
     * @param bucket the index of the hashtable bucket
     * @param depth the number of entries in this hashtable bucket
     * @param mem counted memory used by this hash table
     */
    virtual void visit(int bucket, int depth, size_t mem) = 0;
};

/**
 * Hash table visitor that finds the min and max bucket depths.
 */
class HashTableDepthStatVisitor : public HashTableDepthVisitor {
public:

    HashTableDepthStatVisitor() : depthHisto(GrowingWidthGenerator<unsigned int>(1, 1, 1.3),
                                             10),
                                  size(0), memUsed(0), min(-1), max(0) {}

    void visit(int bucket, int depth, size_t mem) {
        (void)bucket;
        // -1 is a special case for min.  If there's a value other than
        // -1, we prefer that.
        min = std::min(min == -1 ? depth : min, depth);
        max = std::max(max, depth);
        depthHisto.add(depth);
        size += depth;
        memUsed += mem;
    }

    Histogram<unsigned int> depthHisto;
    size_t                  size;
    size_t                  memUsed;
    int                     min;
    int                     max;
};

/**
 * Hash table visitor that collects stats of what's inside.
 */
class HashTableStatVisitor : public HashTableVisitor {
public:

    HashTableStatVisitor() : numNonResident(0), numTotal(0),
                             memSize(0), cacheSize(0) {}

    void visit(StoredValue *v) {
        ++numTotal;
        memSize += v->size();

        if (v->isResident()) {
            cacheSize += v->size();
        } else {
            ++numNonResident;
        }
    }

    size_t numNonResident;
    size_t numTotal;
    size_t memSize;
    size_t cacheSize;
};

/**
 * Track the current number of hashtable visitors.
 *
 * This class is a pretty generic counter holder that increments on
 * entry and decrements on return providing RAII guarantees around an
 * atomic counter.
 */
class VisitorTracker {
public:

    /**
     * Mark a visitor as visiting.
     *
     * @param c the counter that should be incremented (and later
     * decremented).
     */
    explicit VisitorTracker(Atomic<size_t> *c) : counter(c) {
        counter->incr(1);
    }
    ~VisitorTracker() {
        counter->decr(1);
    }
private:
    Atomic<size_t> *counter;
};

/**
 * Types of stored values.
 */
enum stored_value_type {
    small,                      //!< Small (minimally featured) stored values.
    featured                    //!< Full featured stored values.
};

/**
 * Creator of StoredValue instances.
 */
class StoredValueFactory {
public:

    /**
     * Create a new StoredValueFactory of the given type.
     */
    StoredValueFactory(EPStats &s, enum stored_value_type t = featured) : stats(&s),type(t) { }

    /**
     * Create a new StoredValue with the given item.
     *
     * @param itm the item the StoredValue should contain
     * @param n the the top of the hash bucket into which this will be inserted
     * @param ht the hashtable that will contain the StoredValue instance created
     * @param setDirty if true, mark this item as dirty after creating it
     */
    StoredValue *operator ()(const Item &itm, StoredValue *n, HashTable &ht,
                             bool setDirty = true) {
        switch(type) {
        case small:
            return newStoredValue(itm, n, ht, setDirty, true);
            break;
        case featured:
            return newStoredValue(itm, n, ht, setDirty, false);
            break;
        default:
            abort();
        };
    }

private:

    StoredValue* newStoredValue(const Item &itm, StoredValue *n, HashTable &ht,
                                bool setDirty, bool small) {
        size_t base = StoredValue::sizeOf(small);

        std::string key = itm.getKey();
        assert(key.length() < 256);
        size_t len = key.length() + base;

        StoredValue *t = new (::operator new(len))
            StoredValue(itm, n, *stats, ht, setDirty, small);
        if (small) {
            std::memcpy(t->extra.small.keybytes, key.data(), key.length());
        } else {
            std::memcpy(t->extra.feature.keybytes, key.data(), key.length());
        }

        return t;
    }

    EPStats                *stats;
    enum stored_value_type  type;

};

/**
 * A container of StoredValue instances.
 */
class HashTable {
public:

    /**
     * Create a HashTable.
     *
     * @param st the global stats reference
     * @param s the number of hash table buckets
     * @param l the number of locks in the hash table
     * @param t the type of StoredValues this hash table will contain
     */
    HashTable(EPStats &st, size_t s = 0, size_t l = 0,
              enum stored_value_type t = featured) : stats(st), valFact(st, t) {
        size = HashTable::getNumBuckets(s);
        n_locks = HashTable::getNumLocks(l);
        valFact = StoredValueFactory(st, getDefaultStorageValueType());
        assert(size > 0);
        assert(n_locks > 0);
        assert(visitors == 0);
        values = static_cast<StoredValue**>(calloc(size, sizeof(StoredValue*)));
        mutexes = new Mutex[n_locks];
        activeState = true;
    }

    ~HashTable() {
        clear(true);
        // Wait for any outstanding visitors to finish.
        while (visitors > 0) {
            usleep(100);
        }
        delete []mutexes;
        free(values);
        values = NULL;
    }

    size_t memorySize() {
        return sizeof(HashTable)
            + (size * sizeof(StoredValue*))
            + (n_locks * sizeof(Mutex));
    }

    /**
     * Get the number of hash table buckets this hash table has.
     */
    size_t getSize(void) { return size; }

    /**
     * Get the number of locks in this hash table.
     */
    size_t getNumLocks(void) { return n_locks; }

    /**
     * Get the number of items within this hash table.
     */
    size_t getNumItems(void) { return numItems; }

    /**
     * Get the number of non-resident items within this hash table.
     */
    size_t getNumNonResidentItems(void) { return numNonResidentItems; }

    /**
     * Get the number of items whose values are ejected from this hash table.
     */
    size_t getNumEjects(void) { return numEjects; }

    /**
     * Get the total item memory size in this hash table.
     */
    size_t getItemMemory(void) { return memSize; }

    /**
     * Clear the hash table.
     *
     * @param deactivate true when this hash table is being destroyed completely
     *
     * @return a stat visitor reporting how much stuff was removed
     */
    HashTableStatVisitor clear(bool deactivate = false);

    /**
     * Get the number of times this hash table has been resized.
     */
    size_t getNumResizes() { return numResizes; }

    /**
     * Automatically resize to fit the current data.
     */
    void resize();

    /**
     * Resize to the specified size.
     */
    void resize(size_t to);

    /**
     * Find the item with the given key.
     *
     * @param key the key to find
     * @return a pointer to a StoredValue -- NULL if not found
     */
    StoredValue *find(std::string &key) {
        assert(active());
        int bucket_num(0);
        LockHolder lh = getLockedBucket(key, &bucket_num);
        return unlocked_find(key, bucket_num);
    }

    /**
     * During restore from backup we read the most recent values first
     * and works our way back until epoch.. We should therefore only
     * add values to the backup if they're not there;
     *
     * @return true if added, false if skipped
     */
    bool addUnlessThere(const std::string &key,
                        uint16_t vbid,
                        enum queue_operation op,
                        value_t value,
                        uint32_t flags,
                        time_t exptime,
                        uint64_t cas)
    {
        int bucket_num(0);
        LockHolder lh = getLockedBucket(key, &bucket_num);
        if (unlocked_find(key, bucket_num, true)) {
            // it's already there...
            return false;
        }

        // @todo fix this, we're copying the values twice!
        Item itm(key, flags, exptime, value, cas, -1, vbid);
        StoredValue *v = valFact(itm, values[bucket_num], *this);
        assert(v);
        values[bucket_num] = v;
        ++numItems;
        if (op == queue_op_del) {
            unlocked_softDelete(key, cas, bucket_num);
        }
        return true;
    }

    /**
     * Set a new Item into this hashtable.
     *
     * @param val the Item to store
     * @param row_id the row id that is assigned to the item to store
     * @return a result indicating the status of the store
     */
    mutation_type_t set(const Item &val, int64_t &row_id) {
        assert(active());
        Item &itm = const_cast<Item&>(val);
        if (!StoredValue::hasAvailableSpace(stats, itm)) {
            return NOMEM;
        }

        mutation_type_t rv = NOT_FOUND;
        int bucket_num(0);
        LockHolder lh = getLockedBucket(val.getKey(), &bucket_num);
        StoredValue *v = unlocked_find(val.getKey(), bucket_num, true);
        /*
         * prior to checking for the lock, we should check if this object
         * has expired. If so, then check if CAS value has been provided
         * for this set op. In this case the operation should be denied since
         * a cas operation for a key that doesn't exist is not a very cool
         * thing to do. See MB 3252
         */
        bool skip_lock = false;
        if (v && v->isExpired(ep_real_time())) {
            if (val.getCas()) {
                /* item has expired and cas value provided. Deny ! */
                return NOT_FOUND;
            }
            /*
             * proceed to treat this case as if the key never existed
             * therefore skip lock checks
             */
            skip_lock = true;
        }
        if (v && !skip_lock) {
            if (v->isLocked(ep_current_time())) {
                /*
                 * item is locked, deny if there is cas value mismatch
                 * or no cas value is provided by the user
                 */
                if (val.getCas() != v->getCas()) {
                    return IS_LOCKED;
                }
                /* allow operation*/
                v->unlock();
            } else if (val.getCas() != 0 && val.getCas() != v->getCas()) {
                return INVALID_CAS;
            }

            itm.setCas();
            rv = v->isClean() ? WAS_CLEAN : WAS_DIRTY;
            if (!v->isResident()) {
                --numNonResidentItems;
            }
            v->setValue(itm.getValue(),
                        itm.getFlags(), itm.getExptime(),
                        itm.getCas(), stats, *this);
            row_id = v->getId();
        } else {
            if (itm.getCas() != 0) {
                return NOT_FOUND;
            }

            itm.setCas();
            v = valFact(itm, values[bucket_num], *this);
            values[bucket_num] = v;
            ++numItems;
        }
        return rv;
    }

    /**
     * Add an item to the hash table iff it doesn't already exist.
     *
     * @param val the item to store
     * @param isDirty true if the item should be marked dirty on store
     * @param storeVal true if the value should be stored (paged-in)
     * @return an indication of what happened
     */
    add_type_t add(const Item &val, bool isDirty = true, bool storeVal = true) {
        assert(active());
        int bucket_num(0);
        LockHolder lh = getLockedBucket(val.getKey(), &bucket_num);
        StoredValue *v = unlocked_find(val.getKey(), bucket_num, true);
        add_type_t rv = ADD_SUCCESS;
        if (v && !v->isDeleted() && !v->isExpired(ep_real_time())) {
            rv = ADD_EXISTS;
        } else {
            Item &itm = const_cast<Item&>(val);
            itm.setCas();
            if (!StoredValue::hasAvailableSpace(stats, itm)) {
                return ADD_NOMEM;
            }
            if (v) {
                rv = (v->isDeleted() || v->isExpired(ep_real_time())) ? ADD_UNDEL : ADD_SUCCESS;
                v->setValue(itm.getValue(),
                            itm.getFlags(), itm.getExptime(),
                            itm.getCas(), stats, *this);
                if (isDirty) {
                    v->markDirty();
                } else {
                    v->markClean(NULL);
                }
            } else {
                v = valFact(itm, values[bucket_num], *this, isDirty);
                values[bucket_num] = v;
                ++numItems;
            }
            if (!storeVal) {
                v->ejectValue(stats, *this);
            }
        }

        return rv;
    }

    /**
     * Mark the given record logically deleted.
     *
     * @param key the key of the item to delete
     * @param cas the expected CAS of the item (or 0 to override)
     * @param row_id the row id that is assigned to the item to be deleted
     * @return an indicator of what the deletion did
     */
    mutation_type_t softDelete(const std::string &key, uint64_t cas,
                               int64_t &row_id) {
        assert(active());
        int bucket_num(0);
        LockHolder lh = getLockedBucket(key, &bucket_num);
        StoredValue *v = unlocked_find(key, bucket_num);
        if (v) {
            row_id = v->getId();
        }
        return unlocked_softDelete(key, cas, bucket_num);
    }

    /**
     * Unlocked implementation of softDelete.
     */
    mutation_type_t unlocked_softDelete(const std::string &key, uint64_t cas,
                                        int bucket_num) {
        mutation_type_t rv = NOT_FOUND;
        StoredValue *v = unlocked_find(key, bucket_num);
        if (v) {
            if (v->isExpired(ep_real_time())) {
                v->del(stats, *this);
                return rv;
            }

            if (v->isLocked(ep_current_time())) {
                return IS_LOCKED;
            }

            if (cas != 0 && cas != v->getCas()) {
                return NOT_FOUND;
            }

            if (!v->isResident()) {
                --numNonResidentItems;
            }

            /* allow operation*/
            v->unlock();

            rv = v->isClean() ? WAS_CLEAN : WAS_DIRTY;
            v->del(stats, *this);
        }
        return rv;
    }

    /**
     * Find an item within a specific bucket assuming you already
     * locked the bucket.
     *
     * @param key the key of the item to find
     * @param bucket_num the bucket number
     * @param wantsDeleted true if soft deleted items should be returned
     *
     * @return a pointer to a StoredValue -- NULL if not found
     */
    StoredValue *unlocked_find(const std::string &key, int bucket_num,
                               bool wantsDeleted=false) {
        StoredValue *v = values[bucket_num];
        while (v) {
            if (v->hasKey(key)) {
                if (wantsDeleted || !v->isDeleted()) {
                    return v;
                } else {
                    return NULL;
                }
            }
            v = v->next;
        }
        return NULL;
    }

    /**
     * Compute a hash for the given string.
     *
     * @param str the beginning of the string
     * @param len the number of bytes in the string
     *
     * @return the hash value
     */
    inline int hash(const char *str, const size_t len) {
        assert(active());
        int h=5381;

        for(size_t i=0; i < len; i++) {
            h = ((h << 5) + h) ^ str[i];
        }

        return h;
    }

    /**
     * Compute a hash for the given string.
     *
     * @param s the string
     * @return the hash value
     */
    inline int hash(const std::string &s) {
        return hash(s.data(), s.length());
    }

    /**
     * Get a lock holder holding a lock for the bucket for the given
     * hash.
     *
     * @param h the input hash
     * @param bucket output parameter to receive a bucket
     * @return a locked LockHolder
     */
    inline LockHolder getLockedBucket(int h, int *bucket) {
        while (true) {
            assert(active());
            *bucket = getBucketForHash(h);
            LockHolder rv(mutexes[mutexForBucket(*bucket)]);
            if (*bucket == getBucketForHash(h)) {
                return rv;
            }
        }
    }

    /**
     * Get a lock holder holding a lock for the bucket for the hash of
     * the given key.
     *
     * @param s the start of the key
     * @param n the size of the key
     * @param bucket output parameter to receive a bucket
     * @return a locked LockHolder
     */
    inline LockHolder getLockedBucket(const char *s, size_t n, int *bucket) {
        return getLockedBucket(hash(s, n), bucket);
    }

    /**
     * Get a lock holder holding a lock for the bucket for the hash of
     * the given key.
     *
     * @param s the key
     * @param bucket output parameter to receive a bucket
     * @return a locked LockHolder
     */
    inline LockHolder getLockedBucket(const std::string &s, int *bucket) {
        return getLockedBucket(hash(s.data(), s.size()), bucket);
    }

    /**
     * Delete a key from the cache without trying to lock the cache first
     * (Please note that you <b>MUST</b> acquire the mutex before calling
     * this function!!!
     *
     * @param key the key to delete
     * @param bucket_num the bucket to look in (must already be locked)
     * @return true if an object was deleted, false otherwise
     */
    bool unlocked_del(const std::string &key, int bucket_num) {
        assert(active());
        StoredValue *v = values[bucket_num];

        // Special case empty bucket.
        if (!v) {
            return false;
        }

        // Special case the first one
        if (v->hasKey(key)) {
            if (!v->isDeleted() && v->isLocked(ep_current_time())) {
                return false;
            }
            values[bucket_num] = v->next;
            v->reduceCurrentSize(stats, *this, v->size());
            delete v;
            --numItems;
            return true;
        }

        while (v->next) {
            if (v->next->hasKey(key)) {
                StoredValue *tmp = v->next;
                if (!v->isDeleted() && tmp->isLocked(ep_current_time())) {
                    return false;
                }
                v->next = v->next->next;
                tmp->reduceCurrentSize(stats, *this, tmp->size());
                delete tmp;
                --numItems;
                return true;
            } else {
                v = v->next;
            }
        }

        return false;
    }

    /**
     * Delete the item with the given key.
     *
     * @param key the key to delete
     * @return true if the item existed before this call
     */
    bool del(const std::string &key) {
        assert(active());
        int bucket_num(0);
        LockHolder lh = getLockedBucket(key, &bucket_num);
        return unlocked_del(key, bucket_num);
    }

    /**
     * Visit all items within this hashtable.
     */
    void visit(HashTableVisitor &visitor);

    /**
     * Visit all items within this call with a depth visitor.
     */
    void visitDepth(HashTableDepthVisitor &visitor);

    /**
     * Get the number of buckets that should be used for initialization.
     *
     * @param s if 0, return the default number of buckets, else return s
     */
    static size_t getNumBuckets(size_t s = 0);

    /**
     * Get the number of locks that should be used for initialization.
     *
     * @param s if 0, return the default number of locks, else return s
     */
    static size_t getNumLocks(size_t s);

    /**
     * Set the default number of buckets.
     */
    static void setDefaultNumBuckets(size_t);

    /**
     * Set the default number of locks.
     */
    static void setDefaultNumLocks(size_t);

    /**
     * Set the stored value type by name.
     *
     * @param t either "small" or "featured"
     *
     * @return true if this type is not handled.
     */
    static bool setDefaultStorageValueType(const char *t);

    /**
     * Set the default StoredValue type by enum value.
     */
    static void setDefaultStorageValueType(enum stored_value_type);

    /**
     * Get the default StoredValue type.
     */
    static enum stored_value_type getDefaultStorageValueType();

    /**
     * Get the default StoredValue type as a string.
     */
    static const char* getDefaultStorageValueTypeStr();

    Atomic<size_t>       numNonResidentItems;
    Atomic<size_t>       numEjects;
    //! Memory consumed by items in this hashtable.
    Atomic<size_t>       memSize;
    //! Cache size.
    Atomic<size_t>       cacheSize;

private:
    inline bool active() { return activeState = true; }
    inline void active(bool newv) { activeState = newv; }

    size_t               size;
    size_t               n_locks;
    StoredValue        **values;
    Mutex               *mutexes;
    EPStats&             stats;
    StoredValueFactory   valFact;
    Atomic<size_t>       visitors;
    Atomic<size_t>       numItems;
    Atomic<size_t>       numResizes;
    bool                 activeState;

    static size_t                 defaultNumBuckets;
    static size_t                 defaultNumLocks;
    static enum stored_value_type defaultStoredValueType;

    int getBucketForHash(int h) {
        return abs(h % static_cast<int>(size));
    }

    inline int mutexForBucket(int bucket_num) {
        assert(active());
        assert(bucket_num >= 0);
        int lock_num = bucket_num % static_cast<int>(n_locks);
        assert(lock_num < static_cast<int>(n_locks));
        assert(lock_num >= 0);
        return lock_num;
    }

    DISALLOW_COPY_AND_ASSIGN(HashTable);
};

#endif /* STORED_VALUE_H */
