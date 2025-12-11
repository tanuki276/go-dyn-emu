package model

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
)

const KeySeparator = "#"
const GSIKeySeparator = "$"
const SnapshotDir = "dynamodb_snapshots"

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

func ParseNumber(numStr string) (*big.Float, error) {
	f := new(big.Float)
	f.SetPrec(256) 
	if _, _, err := f.Parse(numStr, 10); err != nil {
		return nil, fmt.Errorf("invalid number format: %s", numStr)
	}
	return f, nil
}

func CompareAttributeValuesByOperator(val1 AttributeValue, val2 AttributeValue, op string) (bool, error) {
	val1Str, ok1 := GetAttributeValueString(val1)
	val2Str, ok2 := GetAttributeValueString(val2)

	if !ok1 || !ok2 {
		return false, fmt.Errorf("comparison requires 'S' or 'N' type")
	}

	isNumber := strings.Contains(fmt.Sprintf("%v", val1), "\"N\"") && strings.Contains(fmt.Sprintf("%v", val2), "\"N\"")

	if isNumber {
		n1, err1 := ParseNumber(val1Str)
		n2, err2 := ParseNumber(val2Str)
		if err1 != nil || err2 != nil {
			return false, fmt.Errorf("invalid number for comparison: %v, %v", err1, err2)
		}

		comp := n1.Cmp(n2)
		
		switch op {
		case "=":
			return comp == 0, nil
		case "<":
			return comp < 0, nil
		case ">":
			return comp > 0, nil
		case "<=":
			return comp <= 0, nil
		case ">=":
			return comp >= 0, nil
		default:
			return false, fmt.Errorf("unsupported comparison operator for number: %s", op)
		}
	} else {
		comp := strings.Compare(val1Str, val2Str)
		
		switch op {
		case "=":
			return comp == 0, nil
		case "<":
			return comp < 0, nil
		case ">":
			return comp > 0, nil
		case "<=":
			return comp <= 0, nil
		case ">=":
			return comp >= 0, nil
		default:
			return false, fmt.Errorf("unsupported comparison operator for string: %s", op)
		}
	}
}

func UnionSets(currentAV AttributeValue, addAV AttributeValue) AttributeValue {
	if addSet, ok := addAV["SS"].([]interface{}); ok {
		currentSet, _ := GetStringSet(currentAV)
		
		setMap := make(map[string]struct{})
		for _, s := range currentSet {
			setMap[s] = struct{}{}
		}
		for _, item := range addSet {
			if str, isStr := item.(string); isStr {
				setMap[str] = struct{}{}
			}
		}

		newSet := make([]interface{}, 0, len(setMap))
		for s := range setMap {
			newSet = append(newSet, s)
		}
		return AttributeValue{"SS": newSet}
	}
	
	if addSet, ok := addAV["NS"].([]interface{}); ok {
		currentSet, _ := GetNumberSet(currentAV)

		setMap := make(map[string]struct{})
		for _, n := range currentSet {
			setMap[n] = struct{}{}
		}
		for _, item := range addSet {
			if str, isStr := item.(string); isStr {
				setMap[str] = struct{}{}
			}
		}

		newSet := make([]interface{}, 0, len(setMap))
		for n := range setMap {
			newSet = append(newSet, n)
		}
		return AttributeValue{"NS": newSet}
	}

	if addSet, ok := addAV["BS"].([]interface{}); ok {
		currentSet, _ := GetBinarySet(currentAV)

		setMap := make(map[string]struct{})
		for _, b := range currentSet {
			setMap[b] = struct{}{}
		}
		for _, item := range addSet {
			if str, isStr := item.(string); isStr {
				setMap[str] = struct{}{}
			}
		}

		newSet := make([]interface{}, 0, len(setMap))
		for b := range setMap {
			newSet = append(newSet, b)
		}
		return AttributeValue{"BS": newSet}
	}
	
	return currentAV
}

func SubtractSets(currentAV AttributeValue, deleteAV AttributeValue) (AttributeValue, error) {
	if deleteSet, ok := deleteAV["SS"].([]interface{}); ok {
		currentSet, _ := GetStringSet(currentAV)
		
		deleteMap := make(map[string]struct{})
		for _, item := range deleteSet {
			if str, isStr := item.(string); isStr {
				deleteMap[str] = struct{}{}
			} else {
				return nil, fmt.Errorf("delete set item is not a string")
			}
		}

		newSet := make([]interface{}, 0)
		for _, s := range currentSet {
			if _, isDeleted := deleteMap[s]; !isDeleted {
				newSet = append(newSet, s)
			}
		}
		
		if len(newSet) == 0 {
			return nil, nil 
		}
		return AttributeValue{"SS": newSet}, nil
	}

	if deleteSet, ok := deleteAV["NS"].([]interface{}); ok {
		currentSet, _ := GetNumberSet(currentAV)
		
		deleteMap := make(map[string]struct{})
		for _, item := range deleteSet {
			if str, isStr := item.(string); isStr {
				deleteMap[str] = struct{}{}
			} else {
				return nil, fmt.Errorf("delete set item is not a number string")
			}
		}

		newSet := make([]interface{}, 0)
		for _, n := range currentSet {
			if _, isDeleted := deleteMap[n]; !isDeleted {
				newSet = append(newSet, n)
			}
		}
		
		if len(newSet) == 0 {
			return nil, nil 
		}
		return AttributeValue{"NS": newSet}, nil
	}

	if deleteSet, ok := deleteAV["BS"].([]interface{}); ok {
		currentSet, _ := GetBinarySet(currentAV)
		
		deleteMap := make(map[string]struct{})
		for _, item := range deleteSet {
			if str, isStr := item.(string); isStr {
				deleteMap[str] = struct{}{}
			} else {
				return nil, fmt.Errorf("delete set item is not a binary string")
			}
		}

		newSet := make([]interface{}, 0)
		for _, b := range currentSet {
			if _, isDeleted := deleteMap[b]; !isDeleted {
				newSet = append(newSet, b)
			}
		}
		
		if len(newSet) == 0 {
			return nil, nil 
		}
		return AttributeValue{"BS": newSet}, nil
	}

	return currentAV, fmt.Errorf("delete operation attempted on unsupported type")
}
