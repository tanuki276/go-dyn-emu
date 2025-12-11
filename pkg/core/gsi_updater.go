// pkg/core/gsi_updater.go
package core

import (
    "fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
)

func UpdateGSI(batch *leveldb.Batch, schema model.TableSchema, oldRecord model.Record, newRecord model.Record) {
	if len(schema.GSIs) == 0 {
		return
	}

	for _, gsiSchema := range schema.GSIs {
		oldPKVal, oldSKVal, oldExists := getGSIKeyValues(oldRecord, gsiSchema)
		newPKVal, newSKVal, newExists := getGSIKeyValues(newRecord, gsiSchema)

		if oldExists {
            basePKAV, _ := oldRecord[schema.PartitionKey]
            basePKVal, _ := model.GetAttributeValueString(basePKAV)
            
			oldGSIKey := model.BuildGSILevelDBKey(gsiSchema.IndexName, oldPKVal, oldSKVal, basePKVal)
			batch.Delete([]byte(oldGSIKey))
		}

		if newExists {
            basePKAV, _ := newRecord[schema.PartitionKey]
            basePKVal, _ := model.GetAttributeValueString(basePKAV)

            newGSIKey := model.BuildGSILevelDBKey(gsiSchema.IndexName, newPKVal, newSKVal, basePKVal)
			
            gsiValue := []byte("{}") 
            
            batch.Put([]byte(newGSIKey), gsiValue)
		}
	}
}

func getGSIKeyValues(record model.Record, gsiSchema model.GsiSchema) (string, string, bool) {
	if record == nil {
		return "", "", false
	}
	
	pkAV, pkExists := record[gsiSchema.PartitionKey]
	if !pkExists {
		return "", "", false
	}
	pkVal, _ := model.GetAttributeValueString(pkAV)

	var skVal string
	skExists := true
	if gsiSchema.SortKey != "" {
		skAV, ok := record[gsiSchema.SortKey]
		if ok {
			skVal, _ = model.GetAttributeValueString(skAV)
		} else {
			skExists = false
		}
	}
    
	return pkVal, skVal, pkExists && (gsiSchema.SortKey == "" || skExists)
}
