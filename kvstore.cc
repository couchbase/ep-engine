/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <string>
#include <map>

#include "common.hh"
#include "ep_engine.h"
#include "stats.hh"
#include "kvstore.hh"
#include "sqlite-kvstore.hh"
#include "mc-kvstore/mc-kvstore.hh"

static const char *stringToCharPtr(std::string str) {
    if (!str.empty()) {
        return strdup(str.c_str());
    }
    return NULL;
}

KVStore *KVStoreFactory::create(EventuallyPersistentEngine &theEngine) {
    Configuration &c = theEngine.getConfiguration();

    std::string backend = c.getBackend();
    if (backend.compare("sqlite") == 0) {
        SqliteStrategy *sqliteInstance = NULL;
        enum db_type type;

        if (!KVStoreFactory::stringToType(c.getDbStrategy(), type)) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                    "Unhandled db type: %s", c.getDbStrategy().c_str());
            return NULL;
        }

        switch (type) {
        case multi_db:
            sqliteInstance = new MultiDBSingleTableSqliteStrategy(
                    stringToCharPtr(c.getDbname()),
                    stringToCharPtr(c.getShardpattern()),
                    stringToCharPtr(c.getInitfile()),
                    stringToCharPtr(c.getPostInitfile()), c.getDbShards());
            break;
        case single_db:
            sqliteInstance = new SingleTableSqliteStrategy(
                    stringToCharPtr(c.getDbname()),
                    stringToCharPtr(c.getInitfile()),
                    stringToCharPtr(c.getPostInitfile()));
            break;
        case single_mt_db:
            sqliteInstance = new MultiTableSqliteStrategy(
                    stringToCharPtr(c.getDbname()),
                    stringToCharPtr(c.getInitfile()),
                    stringToCharPtr(c.getPostInitfile()), c.getMaxVBuckets());
            break;
        case multi_mt_db:
            sqliteInstance = new ShardedMultiTableSqliteStrategy(
                    stringToCharPtr(c.getDbname()),
                    stringToCharPtr(c.getShardpattern()),
                    stringToCharPtr(c.getInitfile()),
                    stringToCharPtr(c.getPostInitfile()), c.getMaxVBuckets(),
                    c.getDbShards());
            break;
        case multi_mt_vb_db:
            sqliteInstance = new ShardedByVBucketSqliteStrategy(
                    stringToCharPtr(c.getDbname()),
                    stringToCharPtr(c.getShardpattern()),
                    stringToCharPtr(c.getInitfile()),
                    stringToCharPtr(c.getPostInitfile()), c.getMaxVBuckets(),
                    c.getDbShards());
            break;
        }

        return new StrategicSqlite3(theEngine.getEpStats(),
                shared_ptr<SqliteStrategy> (sqliteInstance));
    } else if (backend.compare("couchdb") == 0) {
        return new MCKVStore(theEngine);
    } else {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, "Unknown backend: [%s]",
                backend.c_str());
    }

    return NULL;
}

static const char* MULTI_DB_NAME("multiDB");
static const char* SINGLE_DB_NAME("singleDB");
static const char* SINGLE_MT_DB_NAME("singleMTDB");
static const char* MULTI_MT_DB_NAME("multiMTDB");
static const char* MULTI_MT_VB_DB_NAME("multiMTVBDB");

const char* KVStoreFactory::typeToString(db_type type) {
    char *rv(NULL);
    switch (type) {
    case multi_db:
        return MULTI_DB_NAME;
        break;
    case single_db:
        return SINGLE_DB_NAME;
        break;
    case single_mt_db:
        return SINGLE_MT_DB_NAME;
        break;
    case multi_mt_db:
        return MULTI_MT_DB_NAME;
        break;
    case multi_mt_vb_db:
        return MULTI_MT_VB_DB_NAME;
        break;
    }
    assert(rv);
    return rv;
}

bool KVStoreFactory::stringToType(std::string name, enum db_type &typeOut) {
    bool rv(true);
    if (name.compare(MULTI_DB_NAME) == 0) {
        typeOut = multi_db;
    } else if (name.compare(SINGLE_DB_NAME) == 0) {
        typeOut = single_db;
    } else if (name.compare(SINGLE_MT_DB_NAME) == 0) {
        typeOut = single_mt_db;
    } else if (name.compare(MULTI_MT_DB_NAME) == 0) {
        typeOut = multi_mt_db;
    } else if (name.compare(MULTI_MT_VB_DB_NAME) == 0) {
        typeOut = multi_mt_vb_db;
    } else {
        rv = false;
    }
    return rv;
}
