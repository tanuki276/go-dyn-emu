package core

import (
	"fmt"
	"strings"

	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
)

func ParseKeyConditionPK(expression, pkName string, values map[string]model.AttributeValue) (string, string, error) {
	parts := strings.Split(expression, " AND ")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		
		operators := []string{"=", "<=", ">=", "<", ">"}
		for _, op := range operators {
			clause := strings.SplitN(part, " " + op + " ", 2)
			if len(clause) == 2 {
				attrPlaceholder := strings.TrimSpace(clause[0])
				valPlaceholder := strings.TrimSpace(clause[1])
				
				isPlaceholder := strings.HasPrefix(attrPlaceholder, "#")
				
				var attrName string
				
				if isPlaceholder {
					
					return "", "", fmt.Errorf("Partition Key expression must use actual attribute name, not placeholder.")
				} else {
					attrName = attrPlaceholder
				}

				if attrName == pkName {
					if !strings.HasPrefix(valPlaceholder, ":") {
						return "", "", fmt.Errorf("Key condition value must be an expression attribute value placeholder")
					}
					
					valAV, ok := values[valPlaceholder]
					if !ok {
						return "", "", fmt.Errorf("Expression attribute value %s not found", valPlaceholder)
					}
					valStr, ok := model.GetAttributeValueString(valAV)
					if !ok {
						return "", "", fmt.Errorf("Partition Key value must be S or N type")
					}
					
					return valStr, op, nil
				}
			}
		}
	}
	return "", "", fmt.Errorf("Partition key condition not found or invalid format.")
}

func ExtractKeyConditionSK(expression, skName string) (string, error) {
	parts := strings.Split(expression, " AND ")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		
		operators := []string{"=", "<=", ">=", "<", ">", "begins_with"}
		for _, op := range operators {
			if strings.Contains(part, op) {
				attrPlaceholder := strings.Split(part, op)[0] 
				attrPlaceholder = strings.TrimSpace(attrPlaceholder)
				
				if strings.Contains(attrPlaceholder, "(") { 
					if strings.HasPrefix(attrPlaceholder, "begins_with(") {
						attrPlaceholder = strings.TrimSuffix(strings.TrimPrefix(attrPlaceholder, "begins_with("), ",")
					}
				}
				
				if attrPlaceholder == skName {
					return part, nil
				}
			}
		}
	}
	return "", nil
}

func EvaluateSortKeyCondition(skVal string, skExpression string, values map[string]model.AttributeValue) bool {
	if skExpression == "" {
		return true
	}
	
	operators := []string{"=", "<=", ">=", "<", ">"}
	
	for _, op := range operators {
		clause := strings.SplitN(skExpression, " " + op + " ", 2)
		if len(clause) == 2 {
			valPlaceholder := strings.TrimSpace(clause[1])
			
			valAV, ok := values[valPlaceholder]
			if !ok {
				return false 
			}
			valStr, ok := model.GetAttributeValueString(valAV)
			if !ok {
				return false
			}

			cond := model.Condition{
				Type: "COMPARISON",
				Operand1: skVal, 
				Operator: op,
				Operand2: valStr, 
			}

			result, _ := model.CompareStringValues(skVal, valStr, op)
			return result
		}
	}

	if strings.HasPrefix(skExpression, "begins_with(") {
		parts := strings.Split(skExpression, ",")
		if len(parts) < 2 {
			return false
		}
		valPlaceholder := strings.TrimSuffix(strings.TrimSpace(parts[1]), ")")

		valAV, ok := values[valPlaceholder]
		if !ok {
			return false
		}
		prefix, ok := model.GetAttributeValueString(valAV)
		if !ok {
			return false
		}
		
		return strings.HasPrefix(skVal, prefix)
	}

	return false
}

func ExtractKey(record model.Record, schema model.TableSchema, indexName string) model.Record {
	key := make(model.Record)
	
	pkName := schema.PartitionKey
	skName := schema.SortKey

	if indexName != "" {
		gsiSchema := schema.GSIs[indexName]
		pkName = gsiSchema.PartitionKey
		skName = gsiSchema.SortKey
		
		if av, ok := record[schema.PartitionKey]; ok {
			key[schema.PartitionKey] = av
		}
	}

	if av, ok := record[pkName]; ok {
		key[pkName] = av
	}
	if skName != "" {
		if av, ok := record[skName]; ok {
			key[skName] = av
		}
	}

	return key
}

