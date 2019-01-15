#include <hubDB/DBMyIndex.h>
#include <hubDB/DBException.h>

using namespace HubDB::Index;
using namespace HubDB::Exception;

LoggerPtr DBMyIndex::logger(Logger::getLogger("HubDB.Index.DBMyIndex"));

// registerClass()-Methode am Ende dieser Datei: macht die Klasse der Factory bekannt
int rMyIdx = DBMyIndex::registerClass();
const BlockNo DBMyIndex::metaBlockNo(0);
extern "C" void * createDBMyIndex(int nArgs, va_list ap);
//TODO: Defininiere Konstante für B+ Baum


DBMyIndex::DBMyIndex(DBBufferMgr &bufferMgr, DBFile &file, enum AttrTypeEnum attrType, ModType mode, bool unique)
        : DBIndex(bufferMgr, file, attrType, mode, unique) {
    if (logger != NULL) {
        LOG4CXX_INFO(logger,"DBMyIndex()");
    }

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
    LOG4CXX_INFO(logger,"~DBMyIndex()");
    unfixBACBs(false);
}

string DBMyIndex::toString(string linePrefix) const {
    //output parent info
    return DBIndex::toString(linePrefix);
}

void DBMyIndex::initializeIndex() {
    LOG4CXX_INFO(logger,"initializeIndex()");
    if (bufMgr.getBlockCnt(file) != 0)
        throw DBIndexException("Can not initialize existing table");

    try {
        bacbStack.push(bufMgr.fixNewBlock(file));
        bacbStack.top().setModified();
        BlockNo * b = (BlockNo *) bacbStack.top().getDataPtr();
        uint * metaPage = (uint *) b + sizeof(BlockNo);
        *metaPage = 0; //depth of tree

        bacbStack.push(bufMgr.fixNewBlock(file));
        bacbStack.top().setModified();
        uint * rootCnt = (uint *) bacbStack.top().getDataPtr();
        *rootCnt = 0; //not sure if this is necessary
        *b = bacbStack.top().getBlockNo();

        LOG4CXX_DEBUG(logger,"Metapage: initial depth "+ TO_STR(*metaPage) +", initial BlockNo "+ TO_STR(*b));
        LOG4CXX_DEBUG(logger,"Keys per inner node: " + TO_STR(keysPerInnerNode()));
        LOG4CXX_DEBUG(logger,"Keys per leaf node: " + TO_STR(keysPerLeafNode()));

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
    return 4;
    //return (DBFileBlock::getBlockSize() - sizeof(uint) - sizeof(BlockNo)) /
    //       (DBAttrType::getSize4Type(attrType) + sizeof(BlockNo));
}

uint DBMyIndex::keysPerLeafNode() const {
    return 4;
    //return (DBFileBlock::getBlockSize() - sizeof(uint)) /
    //       (DBAttrType::getSize4Type(attrType) + sizeof(TID));
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
    uint depth = *((uint *) metaPtr+sizeof(BlockNo));

    LOG4CXX_DEBUG(logger,"Meta Root BlockNo: " + TO_STR(b));
    LOG4CXX_DEBUG(logger,"Meta Depth: "+TO_STR(depth));

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
    LOG4CXX_INFO(logger, "findInInnerNode()");
    LOG4CXX_DEBUG(logger, "BlockNo: "+TO_STR(b));
    bacbStack.push(bufMgr.fixBlock(file, b, LOCK_SHARED));
    const char * ptr = bacbStack.top().getDataPtr();
    uint cnt = *(uint *) ptr;
    if (cnt == 0)
        throw DBIndexException("Empty Inner Node");
    ptr += sizeof(uint);

    //Sequential search, can be replaced with binary
    for (uint i = 0; i < cnt; i++) {
        ptr+=sizeof(BlockNo);
        DBAttrType * attr = DBAttrType::read(ptr, attrType, &ptr);
        if (*attr > val) {
            ptr -= attrTypeSize+sizeof(BlockNo);
            break;
        } else if(*attr == val) {
            break;
        }
    }

    BlockNo result = *((BlockNo *)ptr);
    LOG4CXX_DEBUG(logger, "Found Child BlockNo: "+TO_STR(result));

    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    return result;
}

void DBMyIndex::findInLeafNode(const DBAttrType & val,BlockNo b,list<TID> & tids) {
    LOG4CXX_INFO(logger, "findInLeafNode()");
    LOG4CXX_DEBUG(logger, "BlockNo: "+TO_STR(b));
    bacbStack.push(bufMgr.fixBlock(file, b, LOCK_SHARED));
    const char * ptr = bacbStack.top().getDataPtr();
    uint cnt = *(uint *) ptr;
    if (cnt == 0)
        throw DBIndexException("Empty Leaf Node");

    ptr += sizeof(uint);

    bool found = 0;
    //Sequential search, can be replaced with binary
    for (uint i = 0; i < cnt; i++) {
        DBAttrType * attr = DBAttrType::read(ptr, attrType, &ptr);
        if (*attr == val) {
            TID result = *(TID *) ptr;
            LOG4CXX_DEBUG(logger, "Found TID: "+result.toString());
            tids.push_back(result);
            found = 1;
            break;
        }
        ptr += sizeof(TID);
    }

    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    if(found == 0){
        LOG4CXX_DEBUG(logger, "Value not found");
    }
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

    //Find path to insertion point in leaf node
    char * metaPtr = bacbStack.top().getDataPtr();
    stack<BlockNo> blocks;
    BlockNo b = *(BlockNo *) metaPtr;
    blocks.push(b);
    uint depth = *((uint *) metaPtr+sizeof(BlockNo));
    LOG4CXX_DEBUG(logger,"Tree Depth: "+TO_STR(depth));
    for(int i = 0; i < depth; i++) {
        b = findInInnerNode(val, b);
        blocks.push(b);
    }

    BlockNo leaf = blocks.top();
    blocks.pop();
    LOG4CXX_DEBUG(logger, "Found Leaf Node BlockNo: "+TO_STR(leaf));
    splitInfo splitResult = insertIntoLeaf(leaf, val, tid);
    if(splitResult.splitHappens) {
        LOG4CXX_DEBUG(logger, "Leaf Node "+TO_STR(leaf)+" was split, new Block "+TO_STR(splitResult.newBlockNo)+" was created");
    }
    while(splitResult.splitHappens && !blocks.empty()) {
        BlockNo inner = blocks.top();
        blocks.pop();
        splitResult = insertIntoInner(inner, *splitResult.newKey, splitResult.newBlockNo);
        if(splitResult.splitHappens) {
            LOG4CXX_DEBUG(logger, "Inner Node "+TO_STR(leaf)+" was split, new Block "+TO_STR(splitResult.newBlockNo)+" was created");
        }
    }

    if(splitResult.splitHappens && blocks.empty()) {
        //create new root page
        LOG4CXX_DEBUG(logger,"Root was split, initializing new Root Page");
        bacbStack.push(bufMgr.fixNewBlock(file));
        BlockNo newRootBlockNo = bacbStack.top().getBlockNo();
        LOG4CXX_DEBUG(logger,"New Root BlockNo: " + TO_STR(newRootBlockNo));
        char * newFilePtr = bacbStack.top().getDataPtr();
        uint * newCnt = (uint *) newFilePtr;
        *newCnt = 1;
        LOG4CXX_DEBUG(logger,"New Root Count: " + TO_STR(*newCnt));
        newFilePtr += sizeof(uint);
        BlockNo * newBlock = (BlockNo *) newFilePtr;
        *newBlock = *(BlockNo *) metaPtr; //root node
        LOG4CXX_DEBUG(logger,"New Root Left Node BlockNo: " + TO_STR(*newBlock));
        newFilePtr += sizeof(BlockNo);
        newFilePtr = (*splitResult.newKey).write(newFilePtr);
        LOG4CXX_DEBUG(logger,"New Root Key: " + (*splitResult.newKey).toString());
        newBlock = (BlockNo *) newFilePtr;
        *newBlock = splitResult.newBlockNo;
        LOG4CXX_DEBUG(logger,"New Root Right Node BlockNo: " + TO_STR(*newBlock));
        bacbStack.top().setModified();
        bufMgr.unfixBlock(bacbStack.top());
        bacbStack.pop();

        //modify meta page
        LOG4CXX_DEBUG(logger,"Modifying Meta Page");
        BlockNo * rootBlockNoPtr = (BlockNo *) metaPtr;
        *rootBlockNoPtr = newRootBlockNo;
        LOG4CXX_DEBUG(logger,"New Root BlockNo in Metapage: "+TO_STR(*rootBlockNoPtr));
        uint * depthPtr = (uint *) metaPtr + sizeof(BlockNo);
        *depthPtr = *depthPtr+1;
        LOG4CXX_DEBUG(logger,"New Depth in Metapage: "+TO_STR(*depthPtr));
        bacbStack.top().setModified();
    }

    if (bacbStack.size() != 1)
        throw DBIndexException("BACB Stack is invalid");
}

DBMyIndex::splitInfo DBMyIndex::insertIntoLeaf(const BlockNo b, const DBAttrType &val, const TID &tid) {
    LOG4CXX_INFO(logger,"insertIntoLeaf()");
    LOG4CXX_DEBUG(logger,"BlockNo: "+TO_STR(b));

    splitInfo returnObject;
    returnObject.splitHappens = false;

    bacbStack.push(bufMgr.fixBlock(file, b, LOCK_EXCLUSIVE));
    const char * ptr = bacbStack.top().getDataPtr();
    uint * cnt = (uint *) ptr;
    LOG4CXX_DEBUG(logger, "Keys before Insert: "+TO_STR(*cnt));
    ptr += sizeof(uint);

    uint pos = 0;
    for(; pos < *cnt; pos++) {
        DBAttrType * attr = DBAttrType::read(ptr, attrType, &ptr);
        if (*attr > val) {
            break;
        } else if (*attr == val) {
            TID * tidPtr = (TID *) ptr;
            throw DBIndexException("Insert failed, entry already exists with TID "+(*tidPtr).toString());
            /* TID * tidPtr = (TID *) ptr;
            *tidPtr = tid;
            bacbStack.top().setModified();
            bufMgr.unfixBlock(bacbStack.top());
            bacbStack.pop(); */
            //return returnObject;
        }
        ptr += sizeof(TID);
    }
    LOG4CXX_DEBUG(logger, "Insert Position: "+TO_STR(pos));

    bool unfixNewNode = false;
    if(*cnt == keysPerLeafNode()) {
        LOG4CXX_DEBUG(logger,"Leaf Node full, splitting");

        returnObject.splitHappens = true;
        const char * ptrOld = bacbStack.top().getDataPtr();
        uint keysToMove = keysPerLeafNode()/2;
        LOG4CXX_DEBUG(logger,"KeysToMove: "+TO_STR(keysToMove));
        *cnt -= keysToMove;
        ptrOld += sizeof(uint) + (sizeof(TID)+ attrTypeSize) * *cnt;

        bacbStack.push(bufMgr.fixNewBlock(file));
        char * ptrNew = bacbStack.top().getDataPtr();
        returnObject.newBlockNo = bacbStack.top().getBlockNo();
        LOG4CXX_DEBUG(logger,"New Leaf Node BlockNo: " + TO_STR(returnObject.newBlockNo));

        uint * cntNew = (uint *)ptrNew;
        *cntNew = keysToMove;
        ptrNew+=sizeof(uint);
        memcpy(ptrNew, ptrOld, (sizeof(TID)+ attrTypeSize)*keysToMove);
        returnObject.newKey = DBAttrType::read(ptrNew, attrType);
        LOG4CXX_DEBUG(logger,"New Leaf Node Key: " + returnObject.newKey->toString());

        if(pos <= *cnt) {
            //insert in left node
            LOG4CXX_DEBUG(logger,"Insert into old (left) leaf node");
            bacbStack.top().setModified();
            bufMgr.unfixBlock(bacbStack.top());
            bacbStack.pop();
        } else {
            LOG4CXX_DEBUG(logger,"Insert into new (right) leaf node");
            unfixNewNode = true;
            pos -= *cnt;
        }
    }
    //insert
    LOG4CXX_DEBUG(logger,"Insert in position "+TO_STR(pos)+" in BlockNo "+TO_STR(bacbStack.top().getBlockNo()));
    uint * cntPtr = (uint *) bacbStack.top().getDataPtr();
    char * from = bacbStack.top().getDataPtr() + sizeof(uint) + (sizeof(TID)+ attrTypeSize) * pos;
    if(pos < *cntPtr) {
        //move
        LOG4CXX_DEBUG(logger,"Shifting "+TO_STR(*cntPtr - pos)+" Keys");
        char * to = from + sizeof(TID) + attrTypeSize;
        memmove(to, from, (sizeof(TID) + attrTypeSize) * (*cntPtr - pos));
    }
    from = val.write(from);
    TID * tidPtr = (TID *) from;
    *tidPtr = tid;
    *cntPtr = *cntPtr+1;
    LOG4CXX_DEBUG(logger,"Keys after Insert: " + TO_STR(*cntPtr));


    if(unfixNewNode) {
        bacbStack.top().setModified();
        bufMgr.unfixBlock(bacbStack.top());
        bacbStack.pop();
    }
    bacbStack.top().setModified();
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    return returnObject;
}

DBMyIndex::splitInfo DBMyIndex::insertIntoInner(const BlockNo b, const DBAttrType &val, const BlockNo &newBlockNo) {
    LOG4CXX_INFO(logger,"insertIntoInner()");
    LOG4CXX_DEBUG(logger,"BlockNo: "+TO_STR(b));
    splitInfo returnObject;
    returnObject.splitHappens = false;

    bacbStack.push(bufMgr.fixBlock(file, b, LOCK_EXCLUSIVE));
    const char * ptr = bacbStack.top().getDataPtr();
    uint * cnt = (uint *) ptr;
    LOG4CXX_DEBUG(logger, "Keys before Insert: "+TO_STR(*cnt));
    ptr += sizeof(uint) + sizeof(BlockNo);

    //Linear Search for Insert Position
    uint pos = 0;
    for(; pos < *cnt; pos++) {
        DBAttrType * attr = DBAttrType::read(ptr, attrType, &ptr);
        if (*attr > val) {
            break;
        }
        ptr += sizeof(BlockNo);
    }
    LOG4CXX_DEBUG(logger, "Insert Position: "+TO_STR(pos));

    //if block is full, split
    bool unfixNewNode = false;
    if(*cnt == keysPerInnerNode()) {
        LOG4CXX_DEBUG(logger,"Inner Node full, splitting");
        returnObject.splitHappens = true;
        const char * ptrOld = bacbStack.top().getDataPtr();
        *cnt = keysPerInnerNode() / 2;
        LOG4CXX_DEBUG(logger,"Keys in Left Node: "+TO_STR(*cnt));
        uint keysToMove = keysPerInnerNode() - keysPerInnerNode()/2 - 1;
        LOG4CXX_DEBUG(logger,"Keys in Right Node: "+TO_STR(keysToMove));
        ptrOld += sizeof(uint) + (sizeof(BlockNo)+ attrTypeSize) * ((*cnt)+1);

        bacbStack.push(bufMgr.fixNewBlock(file));
        char * ptrNew = bacbStack.top().getDataPtr();
        returnObject.newBlockNo = bacbStack.top().getBlockNo();
        LOG4CXX_DEBUG(logger,"New Inner Node BlockNo: " + TO_STR(returnObject.newBlockNo));
        uint * cntNew = (uint *)ptrNew;
        *cntNew = keysToMove;
        ptrNew += sizeof(uint);
        memcpy(ptrNew, ptrOld, sizeof(BlockNo) +(sizeof(BlockNo)+ attrTypeSize)*keysToMove);

        ptrOld -= attrTypeSize;
        returnObject.newKey = DBAttrType::read(ptrOld, attrType);
        LOG4CXX_DEBUG(logger,"New Inner Node Key: " + returnObject.newKey->toString());


        if(pos <= *cnt) {
            LOG4CXX_DEBUG(logger,"Insert into old (left) inner node");
            bacbStack.top().setModified();
            bufMgr.unfixBlock(bacbStack.top());
            bacbStack.pop();
        } else {
            LOG4CXX_DEBUG(logger,"Insert into new (right) inner node");
            unfixNewNode = true;
            pos -= *cnt+1;
        }
    }

    //insert
    LOG4CXX_DEBUG(logger,"Insert in position "+TO_STR(pos)+" in BlockNo "+TO_STR(bacbStack.top().getBlockNo()));
    uint * cntPtr = (uint *) bacbStack.top().getDataPtr();
    char * from = bacbStack.top().getDataPtr() + sizeof(uint) + sizeof(BlockNo) + (sizeof(BlockNo)+ attrTypeSize) * pos;
    if(pos < *cntPtr) {
        //move
        LOG4CXX_DEBUG(logger,"Shifting "+TO_STR(*cntPtr - pos)+" Keys");
        char * to = from + sizeof(BlockNo) + attrTypeSize;
        memmove(to, from, (sizeof(BlockNo) + attrTypeSize) * (*cntPtr - pos));
    }
    from = val.write(from);
    BlockNo * blockNoPtr = (BlockNo *) from;
    *blockNoPtr = newBlockNo;
    *cntPtr = *cntPtr+1;
    LOG4CXX_DEBUG(logger,"Keys after Insert: " + TO_STR(*cntPtr));

    if(unfixNewNode) {
        bacbStack.top().setModified();
        bufMgr.unfixBlock(bacbStack.top());
        bacbStack.pop();
    }
    bacbStack.top().setModified();
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    return returnObject;
}

void DBMyIndex::remove(const DBAttrType &val, const DBListTID &tid) {
    LOG4CXX_INFO(logger,"remove()");
    LOG4CXX_DEBUG(logger,"val: "+val.toString());

    // ein Block muss geblockt sein
    if (bacbStack.size() != 1)
        throw DBIndexException("BACB Stack is invalid");
    if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE)
        bufMgr.upgradeToExclusive(bacbStack.top());

    //index is unique, no multiple TIDs
    if(tid.size() > 1) {
        throw DBIndexException("Unique Index Only, no multiple TID delete");
    }

    //Find path to insertion point in leaf node
    char * metaPtr = bacbStack.top().getDataPtr();
    stack<BlockNo> blocks;
    BlockNo b = *(BlockNo *) metaPtr;
    blocks.push(b);
    uint depth = *((uint *) metaPtr+sizeof(BlockNo));
    LOG4CXX_DEBUG(logger,"Tree Depth: "+TO_STR(depth));
    for(int i = 0; i < depth; i++) {
        b = findInInnerNode(val, b);
        blocks.push(b);
    }

    BlockNo parent = blocks.top();
    blocks.pop();
    LOG4CXX_DEBUG(logger, "Found Leaf Node BlockNo: "+TO_STR(parent));

    bool mergeNeeded = removeFromLeafNode(parent, val, tid);
    bool childIsLeaf = true;
    while(mergeNeeded && !blocks.empty()) {
        BlockNo child = parent;
        parent = blocks.top();
        blocks.pop();
        mergeNeeded = rebalanceInnerNode(parent, child, childIsLeaf, blocks.empty());
        childIsLeaf = false;
    }
}

bool DBMyIndex::removeFromLeafNode(const BlockNo b, const DBAttrType &val, const DBListTID &tid) {
    LOG4CXX_INFO(logger,"removeFromLeafNode()");
    LOG4CXX_DEBUG(logger,"BlockNo: "+TO_STR(b));
    LOG4CXX_DEBUG(logger,"val: "+val.toString());

    bacbStack.push(bufMgr.fixBlock(file, b, LOCK_EXCLUSIVE));
    const char * ptr = bacbStack.top().getDataPtr();
    uint * cnt = (uint *) ptr;
    LOG4CXX_DEBUG(logger, "Keys before Delete: "+TO_STR(*cnt));
    ptr += sizeof(uint);

    uint pos = 0;
    bool deleted = false;
    for(; pos < *cnt; pos++) {
        DBAttrType * attr = DBAttrType::read(ptr, attrType, &ptr);
        if (*attr > val) {
            //val already skipped
            break;
        } else if (*attr == val) {
            TID * tidPtr = (TID *) ptr;
            if(*tidPtr == tid.front()) {
                LOG4CXX_DEBUG(logger, "Found at "+TO_STR(pos));
                if(pos != *cnt - 1) {
                    uint keysToMove = *cnt - pos - 1;
                    LOG4CXX_DEBUG(logger,"Shifting "+TO_STR(keysToMove)+" Keys");
                    char * to = (char *)ptr - attrTypeSize;
                    ptr += sizeof(TID);
                    memmove(to, ptr, (sizeof(TID) + attrTypeSize) * keysToMove);
                }
                --*cnt;
                LOG4CXX_DEBUG(logger, "Keys after Delete: "+TO_STR(*cnt));
                deleted = true;
                bacbStack.top().setModified();
            }
        }
        ptr += sizeof(TID);
    }
    if(!deleted)
        LOG4CXX_DEBUG(logger, "Error: Given value not found to delete");
    bool mergeNeeded = *cnt < keysPerLeafNode()/2;
    if(mergeNeeded)
        LOG4CXX_DEBUG(logger, "TODO: Leaf Node is less than half full, merge possibly needed");
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    return mergeNeeded;
}

bool DBMyIndex::rebalanceInnerNode(const BlockNo parentBlockNo, const BlockNo childBlockNo, bool childIsLeaf, bool parentIsRoot) {
    LOG4CXX_INFO(logger,"rebalanceInnerNode()");
    LOG4CXX_DEBUG(logger,"ParentBlockNo: "+TO_STR(parentBlockNo));
    LOG4CXX_DEBUG(logger,"ChildBlockNo: "+TO_STR(childBlockNo));
    LOG4CXX_DEBUG(logger,"ChildIsLeaf: "+TO_STR(childIsLeaf));
    LOG4CXX_DEBUG(logger,"ParentIsRoot: "+TO_STR(parentIsRoot));

    bacbStack.push(bufMgr.fixBlock(file, parentBlockNo, LOCK_EXCLUSIVE));
    const char * ptr = bacbStack.top().getDataPtr();
    uint * cnt = (uint *) ptr;
    ptr += sizeof(uint);

    uint pos = 0;
    for(; pos < *cnt; pos++) {
        BlockNo * currentBlockNo = (BlockNo *) ptr;
        if (*currentBlockNo == childBlockNo) {
            break;
        }
        ptr += sizeof(BlockNo) + attrTypeSize;
    }

    bool mergeFromLeft = true;
    if(pos != 0) {
        //use left node
        LOG4CXX_DEBUG(logger,"Merging from left Node");
        ptr -= sizeof(BlockNo) + attrTypeSize;
    } else {
        //use right node
        LOG4CXX_DEBUG(logger,"Merging from right Node");
        mergeFromLeft = false;
        ptr += sizeof(BlockNo) + attrTypeSize;
    }

    BlockNo child2BlockNo = *(BlockNo *)ptr;
    LOG4CXX_DEBUG(logger,"Child2BlockNo: "+TO_STR(child2BlockNo));
    bacbStack.push(bufMgr.fixBlock(file, child2BlockNo, LOCK_EXCLUSIVE));
    const char * child2Ptr = bacbStack.top().getDataPtr();
    uint * child2Cnt = (uint *) child2Ptr;
    child2Ptr += sizeof(uint);

    LOG4CXX_DEBUG(logger,"child2Cnt: "+TO_STR(*child2Cnt));

    bool moveBlock;
    if(childIsLeaf) {
        moveBlock = *child2Cnt > keysPerLeafNode()/2;
    } else {
        moveBlock = *child2Cnt > keysPerInnerNode()/2;
    }

    if(moveBlock) {
        //move from child2 to child
        LOG4CXX_DEBUG(logger,"Moving one value from "+TO_STR(child2BlockNo)+" to "+TO_STR(childBlockNo));
        if(childIsLeaf) {
            //move a key and TID from one leaf node to another
            DBAttrType * keyToMove;
            TID valueToMove;
            DBAttrType * newParentKey;
            if(mergeFromLeft) {
                --*child2Cnt;
                child2Ptr += (attrTypeSize+sizeof(TID)) * (*child2Cnt);
                keyToMove = DBAttrType::read(child2Ptr, attrType, &child2Ptr);
                valueToMove = *((TID *)child2Ptr);
            } else {
                --*child2Cnt;
                char * to = (char *)child2Ptr;
                keyToMove = DBAttrType::read(child2Ptr, attrType, &child2Ptr);
                valueToMove = *((TID *)child2Ptr);
                child2Ptr += sizeof(TID);
                memmove(to, child2Ptr, (attrTypeSize+sizeof(TID))*(*child2Cnt));
                newParentKey = DBAttrType::read(bacbStack.top().getDataPtr() + sizeof(uint), attrType);
            }
            LOG4CXX_DEBUG(logger,"Key To Move: "+keyToMove->toString());
            LOG4CXX_DEBUG(logger,"Value To Move: "+valueToMove.toString());
            bacbStack.top().setModified();
            bufMgr.unfixBlock(bacbStack.top());
            bacbStack.pop();

            //move pointer to key between the two blocks
            if(mergeFromLeft) {
                ptr += sizeof(BlockNo);
                keyToMove->write((char *)ptr);
            } else {
                ptr -= attrTypeSize;
                newParentKey->write((char *)ptr);
            }
            bacbStack.top().setModified();

            bacbStack.push(bufMgr.fixBlock(file, childBlockNo, LOCK_EXCLUSIVE));
            const char * childPtr = bacbStack.top().getDataPtr();
            uint * childCnt = (uint *) childPtr;
            childPtr += sizeof(uint);
            if(mergeFromLeft) {
                char * to = (char *)childPtr+sizeof(TID)+attrTypeSize;
                memmove(to, childPtr, (sizeof(TID)+attrTypeSize)*(*childCnt));
                keyToMove->write((char *)childPtr);
                TID * childTid = (TID *) childPtr + attrTypeSize;
                *childTid = valueToMove;
            } else {
                childPtr += (*childCnt)*(sizeof(TID)+attrTypeSize);
                keyToMove->write((char *)childPtr);
                TID * childTid = (TID *) childPtr + attrTypeSize;
                *childTid = valueToMove;
            }
            ++*childCnt;
            bacbStack.top().setModified();
            bufMgr.unfixBlock(bacbStack.top());
            bacbStack.pop();
        } else {
            //move a key and BlockNo from one inner node to another
            BlockNo blockNoToMove;
            DBAttrType * newParentKey;
            if(mergeFromLeft) {
                --*child2Cnt;
                child2Ptr += sizeof(BlockNo) + (attrTypeSize+sizeof(BlockNo)) * (*child2Cnt);
                newParentKey = DBAttrType::read(child2Ptr, attrType, &child2Ptr);
                blockNoToMove = *((BlockNo *)child2Ptr);
            } else {
                --*child2Cnt;
                char * to = (char *)child2Ptr;
                blockNoToMove = *((BlockNo *)child2Ptr);
                child2Ptr+=sizeof(BlockNo);
                newParentKey = DBAttrType::read(child2Ptr, attrType, &child2Ptr);
                memmove(to, child2Ptr, sizeof(BlockNo) + (attrTypeSize+sizeof(BlockNo))*(*child2Cnt));
            }
            LOG4CXX_DEBUG(logger,"Key To Move to Parent: "+newParentKey->toString());
            LOG4CXX_DEBUG(logger,"BlockNo To Move to Child: "+TO_STR(blockNoToMove));
            bacbStack.top().setModified();
            bufMgr.unfixBlock(bacbStack.top());
            bacbStack.pop();

            DBAttrType * keyToMove;
            //move pointer to key between the two blocks
            if(mergeFromLeft) {
                ptr += sizeof(BlockNo);
                keyToMove = DBAttrType::read(ptr, attrType);
                newParentKey->write((char *)ptr);
            } else {
                ptr -= attrTypeSize;
                keyToMove = DBAttrType::read(ptr, attrType);
                newParentKey->write((char *)ptr);
            }
            LOG4CXX_DEBUG(logger,"Key To Move to Child: "+keyToMove->toString());
            bacbStack.top().setModified();

            bacbStack.push(bufMgr.fixBlock(file, childBlockNo, LOCK_EXCLUSIVE));
            const char * childPtr = bacbStack.top().getDataPtr();
            uint * childCnt = (uint *) childPtr;
            childPtr += sizeof(uint);
            if(mergeFromLeft) {
                char * to = (char *)childPtr+sizeof(BlockNo)+attrTypeSize;
                memmove(to, childPtr, sizeof(BlockNo) + (sizeof(BlockNo)+attrTypeSize)*(*childCnt));
                BlockNo * childBlockNo = (BlockNo *) childPtr;
                *childBlockNo = blockNoToMove;
                childPtr += sizeof(BlockNo);
                keyToMove->write((char *)childPtr);
            } else {
                childPtr += sizeof(BlockNo) + (*childCnt)*(sizeof(BlockNo)+attrTypeSize);
                keyToMove->write((char *)childPtr);
                BlockNo * childBlockNo = (BlockNo *) childPtr + attrTypeSize;
                *childBlockNo = blockNoToMove;
            }
            ++*childCnt;
            bacbStack.top().setModified();
            bufMgr.unfixBlock(bacbStack.top());
            bacbStack.pop();
        }
    } else {
        LOG4CXX_DEBUG(logger,"Merging Blocks "+TO_STR(child2BlockNo)+" and "+TO_STR(childBlockNo));
        bufMgr.unfixBlock(bacbStack.top());
        bacbStack.pop();

        //merge child and child2
        if(mergeFromLeft) {
            ptr += sizeof(BlockNo);
        } else {
            ptr -= attrTypeSize;
        }
        DBAttrType * keyToMove = DBAttrType::read(ptr, attrType);
        if(pos+1 != *cnt) {
            char * to = (char *) ptr;
            ptr += attrTypeSize + sizeof(BlockNo);
            memmove(to, ptr, (attrTypeSize + sizeof(BlockNo)) * (*cnt - pos - 1));
            --*cnt;
        }
        if(childIsLeaf) {
            if(mergeFromLeft) {
                mergeLeafNodes(child2BlockNo, childBlockNo);
            } else {
                mergeLeafNodes(childBlockNo, child2BlockNo);
            }
        } else {
            if(mergeFromLeft) {
                mergeInnerNodes(child2BlockNo, childBlockNo, *keyToMove);
            } else {
                mergeInnerNodes(childBlockNo, child2BlockNo, *keyToMove);
            }
        }
    }
    if(parentIsRoot) {
        if(*cnt == 0) {
            //merge needed
            BlockNo newRoot = *((BlockNo *) bacbStack.top().getDataPtr() + sizeof(uint));
            bufMgr.unfixBlock(bacbStack.top());
            bacbStack.pop();

            BlockNo * metaRootPtr = (BlockNo *) bacbStack.top().getDataPtr();
            *metaRootPtr = newRoot;
            uint * metaDepthPtr = (uint *) metaRootPtr + sizeof(BlockNo);
            --*metaDepthPtr;
            bacbStack.top().setModified();
        } else {
            bufMgr.unfixBlock(bacbStack.top());
            bacbStack.pop();
        }
        return false;
    }
    bool mergeNeeded = keysPerInnerNode()/2 < *cnt;

    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    return mergeNeeded;
}

void DBMyIndex::mergeInnerNodes(const BlockNo leftNode, const BlockNo rightNode, const DBAttrType &key) {
    LOG4CXX_INFO(logger,"mergeInnerNodes()");
    LOG4CXX_DEBUG(logger,"LeftNode: "+TO_STR(leftNode));
    LOG4CXX_DEBUG(logger,"RightNode: "+TO_STR(rightNode));
    LOG4CXX_DEBUG(logger,"Key in Between: "+key.toString());

    bacbStack.push(bufMgr.fixBlock(file, leftNode, LOCK_EXCLUSIVE));
    char * leftPtr = bacbStack.top().getDataPtr();
    uint * leftCnt = (uint *) leftPtr;
    leftPtr += sizeof(uint) + sizeof(BlockNo) + (attrTypeSize+sizeof(BlockNo)) * (*leftCnt);
    key.write(leftPtr);
    leftPtr += attrTypeSize;

    bacbStack.push(bufMgr.fixBlock(file, rightNode, LOCK_EXCLUSIVE));
    const char * rightPtr = bacbStack.top().getDataPtr();
    uint * rightCnt = (uint *) rightPtr;
    rightPtr += sizeof(uint);

    memcpy(leftPtr, rightPtr, sizeof(BlockNo) + (attrTypeSize+sizeof(BlockNo)) * (*rightCnt));
    *leftCnt += *rightCnt + 1;

    //right node is deleted
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    //left node is saved
    bacbStack.top().setModified();
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();
}

void DBMyIndex::mergeLeafNodes(const BlockNo leftNode, const BlockNo rightNode) {
    LOG4CXX_INFO(logger,"mergeLeafNodes()");
    LOG4CXX_DEBUG(logger,"LeftNode: "+TO_STR(leftNode));
    LOG4CXX_DEBUG(logger,"RightNode: "+TO_STR(rightNode));

    bacbStack.push(bufMgr.fixBlock(file, leftNode, LOCK_EXCLUSIVE));
    char * leftPtr = bacbStack.top().getDataPtr();
    uint * leftCnt = (uint *) leftPtr;
    leftPtr += sizeof(uint) + (attrTypeSize+sizeof(TID)) * (*leftCnt);

    bacbStack.push(bufMgr.fixBlock(file, rightNode, LOCK_EXCLUSIVE));
    const char * rightPtr = bacbStack.top().getDataPtr();
    uint * rightCnt = (uint *) rightPtr;
    rightPtr += sizeof(uint);

    memcpy(leftPtr, rightPtr, (attrTypeSize+sizeof(TID)) * (*rightCnt));
    *leftCnt += *rightCnt;

    //right node is deleted
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    //left node is saved
    bacbStack.top().setModified();
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();
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
extern "C" void * createDBMyIndex(int nArgs, va_list ap) {
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
