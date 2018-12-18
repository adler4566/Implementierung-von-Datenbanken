#include <hubDB/DBMyIndex.h>
#include <hubDB/DBException.h>

using namespace HubDB::Index;
using namespace HubDB::Exception;

LoggerPtr DBMyIndex::logger(Logger::getLogger("HubDB.Index.DBMyIndex"));

// registerClass()-Methode am Ende dieser Datei: macht die Klasse der Factory bekannt
int rMyIdx = DBMyIndex::registerClass();
const BlockNo DBMyIndex::metaBlockNo(0);
extern "C" void * createDBMyIndex(int nArgs, va_list & ap);
//TODO: Defininiere Konstante für B+ Baum

DBMyIndex::DBMyIndex(DBBufferMgr &bufferMgr, DBFile &file, enum AttrTypeEnum attrType, ModType mode, bool unique)
        : DBIndex(bufferMgr, file, attrType, mode, unique) {
    if (logger != NULL) {
        LOG4CXX_INFO(logger,"DBMyIndex()");
    }

    //possibly need to assert meta page size
    assert(keysPerInnerNode()>1);
    assert(keysPerLeafNode()>1);

    if (bufMgr.getBlockCnt(file) == 0) {
        LOG4CXX_DEBUG(logger,"initializeIndex");
        initializeIndex();
    }

    //fix meta block
    bacbStack.push(bufMgr.fixBlock(file, metaBlockNo, mode == READ ? LOCK_SHARED : LOCK_INTWRITE));

    if (logger != NULL) {
        LOG4CXX_DEBUG(logger,"this:\n"+toString("\t"));
    }
}

DBMyIndex::~DBMyIndex() {
    LOG4CXX_INFO(logger,"~DBSeqIndex()");
    unfixBACBs(false);

}

string DBMyIndex::toString(string linePrefix) const {
    return DBIndex::toString(linePrefix);
}

void DBMyIndex::initializeIndex() {
    LOG4CXX_INFO(logger,"initializeIndex()");
    if (bufMgr.getBlockCnt(file) != 0)
        throw DBIndexException("can not initialize existing table");

    try {
        bacbStack.push(bufMgr.fixNewBlock(file));
        bacbStack.top().setModified();
        BlockNo * b = (BlockNo *) bacbStack.top().getDataPtr();
        uint * metaPage = (uint *) b + sizeof(BlockNo);
        metaPage[0] = 0; //depth of tree
        metaPage[1] = keysPerInnerNode(); //branching factor of inner nodes (TBD)
        metaPage[2] = keysPerLeafNode(); //branching factor of leaf nodes (TBD)

        bacbStack.push(bufMgr.fixNewBlock(file));
        bacbStack.top().setModified();
        uint * rootCnt = (uint *) bacbStack.top().getDataPtr();
        rootCnt = 0; //not sure if this is necessary
        b[0] = bacbStack.top().getBlockNo();
    } catch (DBException & e) {
        if (bacbStack.empty() == false)
            bufMgr.unfixBlock(bacbStack.top());
        if (bacbStack.empty() == false)
            bufMgr.unfixBlock(bacbStack.top());
        throw e;
    }
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    // nun muss die liste der geblockten Seiten wieder leer sein, sonst Abbruch
    assert(bacbStack.empty()==true);

}

uint DBMyIndex::keysPerInnerNode() const {
    return (DBFileBlock::getBlockSize() - sizeof(uint) - sizeof(BlockNo)) /
           (DBAttrType::getSize4Type(attrType) + sizeof(BlockNo));
}

uint DBMyIndex::keysPerLeafNode() const {
    return (DBFileBlock::getBlockSize() - sizeof(uint)) /
           (DBAttrType::getSize4Type(attrType) + sizeof(TID));
}

void DBMyIndex::find(const DBAttrType &val, DBListTID &tids) {
    LOG4CXX_INFO(logger,"find()");
    LOG4CXX_DEBUG(logger,"val:\n"+val.toString("\t"));

    // ein Block muss geblockt sein
    if (bacbStack.size() != 1)
        throw DBIndexException("BACB Stack is invalid");

    tids.clear();

    char * metaPtr = bacbStack.top().getDataPtr();
    BlockNo b = *(BlockNo *) metaPtr;
    uint depth = *(uint *) metaPtr+sizeof(BlockNo);

    for(int i = 0; i < depth; i++) {
        //do find for inner nodes and set block to new value
        b = findInInnerNode(val, b);
    }
    //do find for leaf node, set tids to tid
    findInLeafNode(val, b, tids);

    if (bacbStack.size() != 1)
        throw DBIndexException("BACB Stack is invalid");
}

BlockNo DBMyIndex::findInInnerNode(const DBAttrType & val, BlockNo b) {
    bacbStack.push(bufMgr.fixBlock(file, b, LOCK_SHARED));
    const char * ptr = bacbStack.top().getDataPtr();
    uint cnt = *(uint *) ptr;
    if (cnt == 0)
        throw DBIndexException("Empty Inner Node");
    ptr += sizeof(uint)+sizeof(BlockNo);

    for (uint i = 0; i < cnt; i++) {
        DBAttrType * attr = DBAttrType::read(ptr, attrType, &ptr);
        if (*attr > val) {
            ptr -= DBAttrType::getSize4Type(attrType)+sizeof(BlockNo);
            break;
        } else if(*attr == val) {
            break;
        }
        ptr+=sizeof(BlockNo);
    }

    BlockNo result = *(BlockNo *)ptr;

    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    return result;
}

void DBMyIndex::findInLeafNode(const DBAttrType & val,BlockNo b,list<TID> & tids) {
    bacbStack.push(bufMgr.fixBlock(file, b, LOCK_SHARED));
    const char * ptr = bacbStack.top().getDataPtr();
    uint cnt = *(uint *) ptr;
    if (cnt == 0)
        throw DBIndexException("Empty Leaf Node");

    ptr += sizeof(uint);

    //NOTE MAYBE DO ++i HERE, CHECK
    for (uint i = 0; i < cnt; i++) {
        DBAttrType * attr = DBAttrType::read(ptr, attrType, &ptr);
        if (*attr == val) {
            tids.push_back(*(TID *) ptr);
            break;
        }
        ptr += sizeof(TID);
    }

    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();
}

void DBMyIndex::insert(const DBAttrType &val, const TID &tid) {
    LOG4CXX_INFO(logger,"insert()");
    LOG4CXX_DEBUG(logger,"val:\n"+val.toString("\t"));
    LOG4CXX_DEBUG(logger,"tid: "+tid.toString());

    // ein Block muss geblockt sein
    if (bacbStack.size() != 1)
        throw DBIndexException("BACB Stack is invalid");

    if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE)
        bufMgr.upgradeToExclusive(bacbStack.top());

    char * metaPtr = bacbStack.top().getDataPtr();
    stack<BlockNo> blocks;
    BlockNo b = *(BlockNo *) metaPtr;
    blocks.push(b);
    uint depth = *(uint *) metaPtr+sizeof(BlockNo);

    for(int i = 0; i < depth; i++) {
        b = findInInnerNode(val, b);
        blocks.push(b);
    }

    BlockNo leaf = blocks.top();

}

DBMyIndex::splitInfo DBMyIndex::insertIntoLeaf(const BlockNo b, const DBAttrType &val, const TID &tid) {
    splitInfo returnObject;
    returnObject.splitHappens = false;

    bacbStack.push(bufMgr.fixBlock(file, b, LOCK_EXCLUSIVE));
    const char * ptr = bacbStack.top().getDataPtr();
    uint * cnt = (uint *) ptr;
    ptr += sizeof(uint);

    uint pos = 0;
    for(; pos < *cnt; pos++) {
        DBAttrType * attr = DBAttrType::read(ptr, attrType, &ptr);
        if (*attr > val) {
            pos--;
            break;
        } else if (*attr == val) {
            TID * tidPtr = (TID *) ptr;
            *tidPtr = tid;
            return returnObject;
        }
        ptr += sizeof(TID);
    }
    bool unfixNewNode = false;
    if(*cnt == keysPerLeafNode()) {
        //DO A SPLIT!
        returnObject.splitHappens = true;
        const char * ptrOld = bacbStack.top().getDataPtr();
        uint keysToMove = keysPerLeafNode() / 2;
        *cnt -= keysToMove;
        ptrOld += sizeof(uint) + (sizeof(TID)+ attrTypeSize) * *cnt;

        bacbStack.push(bufMgr.fixNewBlock(file));
        char * ptrNew = bacbStack.top().getDataPtr();
        returnObject.newBlockNo = bacbStack.top().getBlockNo();
        uint * cntNew = (uint *)ptrNew;
        *cntNew = keysToMove;
        ptrNew+=sizeof(uint);
        memcpy(ptrNew, ptrOld, (sizeof(TID)+ attrTypeSize)*keysToMove);

        if(pos <= *cnt) {
            //insert in left node

            //TODO set newKey in return object
            //DBAttrType * attr = DBAttrType::read(ptrNew, attrType);
            //returnObject.newKey = attr;

            LOG4CXX_DEBUG(logger,"unfix right page");
            bufMgr.unfixBlock(bacbStack.top());
            bacbStack.pop();
        } else {
            unfixNewNode = true;
            pos -= *cnt;
        }
    }
    //insert
    uint * cntPtr = (uint *) bacbStack.top().getDataPtr();
    char * from = bacbStack.top().getDataPtr() + sizeof(uint) + (sizeof(TID)+ attrTypeSize) * pos;
    if(pos < *cnt) {
        //move
        char * to = from + sizeof(TID) + attrTypeSize;
        memmove(to, from, (sizeof(TID) + attrTypeSize) * (*cnt - pos));
    }
    from = val.write(from);
    TID * tidPtr = (TID *) from;
    *tidPtr = tid;
    *cntPtr++;

    if(unfixNewNode) {
        //TODO set newKey in return object
        bufMgr.unfixBlock(bacbStack.top());
        bacbStack.pop();
    }
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    return returnObject;
}

void DBMyIndex::remove(const DBAttrType &val, const DBListTID &tid) {

}

void DBMyIndex::unfixBACBs(bool setDirty) {
    LOG4CXX_INFO(logger,"unfixBACBs()");
    LOG4CXX_DEBUG(logger,"setDirty: "+TO_STR(setDirty));
    LOG4CXX_DEBUG(logger,"bacbStack.size()= "+TO_STR(bacbStack.size()));
    while (bacbStack.empty() == false) {
        try {
            if (bacbStack.top().getModified()) {
                if (setDirty == true)
                    bacbStack.top().setDirty();
            }
            bufMgr.unfixBlock(bacbStack.top());
        } catch (DBException & e) {
        }
        bacbStack.pop();
    }
}

int DBMyIndex::registerClass() {
    setClassForName("DBMyIndex", createDBMyIndex);
    return 0;
}

/**
 * Gerufen von HubDB::Types::getClassForName von DBTypes, um DBIndex zu erstellen
 * - DBBufferMgr *: Buffermanager
 * - DBFile *: Dateiobjekt
 * - attrType: Attributtp
 * - ModeType: READ, WRITE
 * - bool: unique Indexattribut
 */
extern "C" void * createDBMyIndex(int nArgs, va_list & ap) {
    // Genau 5 Parameter
    if (nArgs != 5) {
        throw DBException("Invalid number of arguments");
    }
    DBBufferMgr * bufMgr = va_arg(ap,DBBufferMgr *);
    DBFile * file = va_arg(ap,DBFile *);
    enum AttrTypeEnum attrType = (enum AttrTypeEnum) va_arg(ap,int);
    ModType m = (ModType) va_arg(ap,int);
    bool unique = (bool) va_arg(ap,int);
    return new DBMyIndex(*bufMgr, *file, attrType, m, unique);
}