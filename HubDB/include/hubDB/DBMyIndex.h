#ifndef HUBDB_DBMYINDEX_H
#define HUBDB_DBMYINDEX_H

#include <hubDB/DBIndex.h>

namespace HubDB{
    namespace Index{
        class DBMyIndex : public DBIndex{

        public:
            DBMyIndex(DBBufferMgr & bufferMgr,DBFile & file,enum AttrTypeEnum attrType,ModType mode,bool unique);
            ~DBMyIndex();
            string toString(string linePrefix="") const;

            void initializeIndex();
            void find(const DBAttrType & val,DBListTID & tids);
            void insert(const DBAttrType & val,const TID & tid);
            void remove(const DBAttrType & val,const DBListTID & tid);
            bool isIndexNonUniqueAble(){ return false;};
            void unfixBACBs(bool dirty);

            static int registerClass();

        private:
            struct splitInfo {
                splitInfo();
                bool splitHappens;
                const DBAttrType & newKey;
                BlockNo newBlockNo;
            };
            uint keysPerInnerNode()const;
            uint keysPerLeafNode()const;

            BlockNo findInInnerNode(const DBAttrType & val, BlockNo b);
            void findInLeafNode(const DBAttrType & val,BlockNo b,list<TID> & tids);

            splitInfo insertIntoLeaf(const BlockNo b, const DBAttrType &val, const TID &tid);

            static const BlockNo metaBlockNo;
            stack<DBBACB> bacbStack;
        };
    }
}

#endif //HUBDB_DBMYINDEX_H
