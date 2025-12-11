package core

import (
	"fmt"
    "strings"
	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
)

type UpdateActions struct {
    Set map[string]model.AttributeValue 
    Add map[string]model.AttributeValue 
    Remove map[string]struct{}         
    Delete map[string]model.AttributeValue 
}


func ParseUpdateExpression(input *model.UpdateItemInput) (*UpdateActions, error) {
    actions := &UpdateActions{
        Set: make(map[string]model.AttributeValue),
        Add: make(map[string]model.AttributeValue),
        Remove: make(map[string]struct{}),
        Delete: make(map[string]model.AttributeValue),
    }

    parts := strings.Split(input.UpdateExpression, " ")

    currentOp := ""

    for i := 0; i < len(parts); i++ {
        part := strings.ToUpper(parts[i])

        if part == "SET" || part == "ADD" || part == "REMOVE" || part == "DELETE" {
            currentOp = part
            continue
        }

        if currentOp == "" {
            continue
        }

        if currentOp == "SET" {
            if i+2 < len(parts) && parts[i+1] == "=" && strings.HasPrefix(parts[i+2], ":") {
                attrPlaceholder := parts[i]
                valPlaceholder := parts[i+2]

                attrName, ok := input.ExpressionAttributeNames[attrPlaceholder]
                if !ok {
                    attrName = attrPlaceholder
                }

                valAV, ok := input.ExpressionAttributeValues[valPlaceholder]
                if !ok {
                    return nil, fmt.Errorf("SET value %s not found in ExpressionAttributeValues", valPlaceholder)
                }

                actions.Set[attrName] = valAV
                i += 2
            }
        } else if currentOp == "ADD" {
            if i+1 < len(parts) && strings.HasPrefix(parts[i+1], ":") {
                attrPlaceholder := parts[i]
                valPlaceholder := parts[i+1]

                attrName, ok := input.ExpressionAttributeNames[attrPlaceholder]
                if !ok {
                    attrName = attrPlaceholder
                }

                valAV, ok := input.ExpressionAttributeValues[valPlaceholder]
                if !ok {
                    return nil, fmt.Errorf("ADD value %s not found in ExpressionAttributeValues", valPlaceholder)
                }

                actions.Add[attrName] = valAV
                i += 1
            }
        } else if currentOp == "REMOVE" {
            attrPlaceholder := parts[i]
            attrName, ok := input.ExpressionAttributeNames[attrPlaceholder]
            if !ok {
                attrName = attrPlaceholder
            }
            actions.Remove[attrName] = struct{}{}
        } else if currentOp == "DELETE" {
            if i+1 < len(parts) && strings.HasPrefix(parts[i+1], ":") {
                attrPlaceholder := parts[i]
                valPlaceholder := parts[i+1]

                attrName, ok := input.ExpressionAttributeNames[attrPlaceholder]
                if !ok {
                    attrName = attrPlaceholder
                }

                valAV, ok := input.ExpressionAttributeValues[valPlaceholder]
                if !ok {
                    return nil, fmt.Errorf("DELETE value %s not found in ExpressionAttributeValues", valPlaceholder)
                }
                
                if _, ok := valAV["SS"]; !ok {
                    if _, ok := valAV["NS"]; !ok {
                        if _, ok := valAV["BS"]; !ok {
                            return nil, fmt.Errorf("DELETE only supports set types (SS, NS, BS)")
                        }
                    }
                }

                actions.Delete[attrName] = valAV
                i += 1
            } else {
                 return nil, fmt.Errorf("Invalid DELETE clause format")
            }
        }
    }

    return actions, nil
}


func ApplyUpdateActions(oldRecord model.Record, actions *UpdateActions) (model.Record, error) {
    newRecord := make(model.Record, len(oldRecord))
    for k, v := range oldRecord {
        newRecord[k] = v
    }

    for k := range actions.Remove {
        delete(newRecord, k)
    }

    for k, v := range actions.Set {
        newRecord[k] = v
    }

    for k, v := range actions.Add {
        currentAV, exists := newRecord[k]

        if _, ok := v["N"]; ok {
            if !exists || currentAV == nil || currentAV["N"] == nil {
                newRecord[k] = v
            } else {
                current, err1 := model.ParseNumber(currentAV["N"].(string))
                add, err2 := model.ParseNumber(v["N"].(string))
                if err1 != nil || err2 != nil {
                    return nil, fmt.Errorf("ADD operation failed: invalid number format for attribute %s", k)
                }
                newRecord[k] = model.AttributeValue{"N": fmt.Sprintf("%v", current.Add(add))}
            }
        } else if _, ok := v["SS"]; ok || _, ok := v["NS"]; ok || _, ok := v["BS"]; ok {
            newAV := model.UnionSets(currentAV, v)
            newRecord[k] = newAV
        } else {
            return nil, fmt.Errorf("ADD operation only supports number or set types for attribute %s", k)
        }
    }

    for k, v := range actions.Delete {
        currentAV, exists := newRecord[k]
        if !exists {
            continue
        }

        newAV, err := model.SubtractSets(currentAV, v)
        if err != nil {
            return nil, fmt.Errorf("DELETE operation failed: %v", err)
        }
        newRecord[k] = newAV
    }

    return newRecord, nil
}
