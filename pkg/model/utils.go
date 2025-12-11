package model

import (
	"encoding/json"
	"fmt"
	"strings"
)

const KeySeparator = "#"
const GSIKeySeparator = "$"

func BuildLevelDBKey(tableName string, pkVal string, skVal string) string {
	if skVal == "" {
		return fmt.Sprintf("%s%s%s", tableName, KeySeparator, pkVal)
	}
	return fmt.Sprintf("%s%s%s%s%s", tableName, KeySeparator, pkVal, KeySeparator, skVal)
}

func BuildGSILevelDBKey(indexName string, gpkVal string, gskVal string, basePkVal string) string {
	if gskVal == "" {
		return fmt.Sprintf("%s%s%s%s%s", indexName, GSIKeySeparator, gpkVal, GSIKeySeparator, basePkVal)
	}
	return fmt.Sprintf("%s%s%s%s%s%s%s", indexName, GSIKeySeparator, gpkVal, GSIKeySeparator, gskVal, GSIKeySeparator, basePkVal)
}

func MarshalRecord(r Record) ([]byte, error) {
	return json.Marshal(r)
}

func UnmarshalRecord(data []byte) (Record, error) {
	var r Record
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return r, nil
}

func GetAttributeValueString(av AttributeValue) (string, bool) {
	if sVal, ok := av["S"].(string); ok {
		return sVal, true
	}
	if nVal, ok := av["N"].(string); ok {
		return nVal, true
	}
	return "", false
}
