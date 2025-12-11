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

func GetNumberSet(av AttributeValue) ([]string, bool) {
	if nsVal, ok := av["NS"].([]interface{}); ok {
		s := make([]string, len(nsVal))
		for i, v := range nsVal {
			if str, ok := v.(string); ok {
				s[i] = str
			} else {
				return nil, false
			}
		}
		return s, true
	}
	return nil, false
}

func GetStringSet(av AttributeValue) ([]string, bool) {
	if ssVal, ok := av["SS"].([]interface{}); ok {
		s := make([]string, len(ssVal))
		for i, v := range ssVal {
			if str, ok := v.(string); ok {
				s[i] = str
			} else {
				return nil, false
			}
		}
		return s, true
	}
	return nil, false
}

func GetBinarySet(av AttributeValue) ([]string, bool) {
	if bsVal, ok := av["BS"].([]interface{}); ok {
		s := make([]string, len(bsVal))
		for i, v := range bsVal {
			if str, ok := v.(string); ok {
				s[i] = str
			} else {
				return nil, false
			}
		}
		return s, true
	}
	return nil, false
}
