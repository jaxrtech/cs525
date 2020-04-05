#include "tables.h"

RC
getAttrOffset (Schema *schema, int attrNum, int *result)
{
    int offset = 0;
    int attrPos = 0;

    for(attrPos = 0; attrPos < attrNum; attrPos++)
        switch (schema->dataTypes[attrPos])
        {
            case DT_STRING:
                offset += schema->typeLength[attrPos];
                break;
            case DT_INT:
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                offset += sizeof(float);
                break;
            case DT_BOOL:
                offset += sizeof(bool);
                break;
        }

    *result = offset;
    return RC_OK;
}
/*
void *printSchema(Schema *s){
    int numAttr;            //relative const
    char **attrNames;       //array of strings
    DataType *dataTypes;    //array of dataTypes
    int *typeLength;        //array of lengths of dataTypes
    int *keyAttrs;          //array of keyAttrs
    int keySize;            //relative const
    int dataPageNum;        //pagenum that stores first tuple of data
    printf("SCHEMA:\n\tnumAttr: %d\n", s->numAttr);
    int i; for (i = 0; i < s->numAttr; ++i){
        printf("\tattrNames[%d]: %s\n", i, s->attrNames[i]);
        printf("\tdataTypes[%d]: %d\n", i, s->dataTypes[i]);
        printf("\ttypeLength[%d]: %d\n", i, s->typeLength[i]);
        printf("\tkeyAttrs[%d]: %d\n\n", i, s->keyAttrs[i]);
    } 
    printf("\tkeySize: %d\n", s->keySize);
    printf("\tdataPageNum: %d\n", s->dataPageNum);
    return;
}
*/