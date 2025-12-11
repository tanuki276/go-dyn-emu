// pkg/core/condition_evaluator.go
package core

import (
	"fmt"
	"strings"
	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
)

type Condition struct {
	Type     string 
	Operand1 string 
	Operator string 
	Operand2 string 
}

func parseCondition(expr string) (Condition, error) {
    expr = strings.TrimSpace(expr)

    if strings.Contains(expr, "(") {
        if strings.HasPrefix(expr, "attribute_exists(") && strings.HasSuffix(expr, ")") {
            return Condition{Type: "FUNCTION", Operator: "attribute_exists", Operand1: strings.TrimSuffix(strings.TrimPrefix(expr, "attribute_exists("), ")")}, nil
        }
        if strings.HasPrefix(expr, "attribute_not_exists(") && strings.HasSuffix(expr, ")") {
            return Condition{Type: "FUNCTION", Operator: "attribute_not_exists", Operand1: strings.TrimSuffix(strings.TrimPrefix(expr, "attribute_not_exists("), ")")}, nil
        }
        return Condition{}, fmt.Errorf("unsupported condition function: %s", expr)
    }

    operators := []string{"<=", ">=", "=", "<", ">"}
    
    for _, op := range operators {
        parts := strings.SplitN(expr, " " + op + " ", 2)
        if len(parts) == 2 {
            return Condition{
                Type: "COMPARISON",
                Operand1: strings.TrimSpace(parts[0]),
                Operator: op,
                Operand2: strings.TrimSpace(parts[1]),
            }, nil
        }
    }

    return Condition{}, fmt.Errorf("unsupported condition expression format: %s", expr)
}


func evaluateSingleCondition(record model.Record, cond Condition, input model.ConditionInput) (bool, error) {
    if cond.Type == "FUNCTION" {
        attrPlaceholder := cond.Operand1
        attrName, ok := input.ExpressionAttributeNames[attrPlaceholder]
        if !ok { attrName = attrPlaceholder } 

        _, exists := record[attrName]

        if cond.Operator == "attribute_exists" {
            return exists, nil
        }
        if cond.Operator == "attribute_not_exists" {
            return !exists, nil
        }
        return false, fmt.Errorf("internal: unknown function operator: %s", cond.Operator)
    }

    if cond.Type == "COMPARISON" {
        attrPlaceholder := cond.Operand1
        valPlaceholder := cond.Operand2
        op := cond.Operator

        attrName, ok := input.ExpressionAttributeNames[attrPlaceholder]
        if !ok { attrName = attrPlaceholder }
        
        valAV, ok := input.ExpressionAttributeValues[valPlaceholder]
        if !ok { return false, fmt.Errorf("ExpressionAttributeValue %s not found", valPlaceholder) }
        
        recordAV, exists := record[attrName]
        if !exists { return false, nil } 

        return model.CompareAttributeValuesByOperator(recordAV, valAV, op)
    }

    return false, fmt.Errorf("internal: unsupported condition type: %s", cond.Type)
}

func EvaluateConditionExpression(record model.Record, input model.ConditionInput) (bool, error) {
	if input.ConditionExpression == "" {
		return true, nil
	}
    
    cond, err := parseCondition(input.ConditionExpression)
    if err != nil {
        return false, err
    }
    
    if record == nil && cond.Operator != "attribute_not_exists" {
        return false, nil
    }

    return evaluateSingleCondition(record, cond, input)
}
