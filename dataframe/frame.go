package dataframe

import (
	"encoding/json"
	"fmt"
)

// Series es una columna genérica
type Series[T any] struct {
	Name string
	Data []T
}

// DataFrame es el contenedor de columnas
type DataFrame struct {
	Columns map[string]interface{}
}

func NewDataFrame() *DataFrame {
	return &DataFrame{Columns: make(map[string]interface{})}
}

func (df *DataFrame) AddSeries(name string, s interface{}) {
	df.Columns[name] = s
}

func (df *DataFrame) GetColumn(name string) (interface{}, error) {
	col, ok := df.Columns[name]
	if !ok {
		return nil, fmt.Errorf("columna %s no encontrada", name)
	}
	return col, nil
}

// SELECT: Crea un nuevo DataFrame solo con las columnas deseadas
func (df *DataFrame) Select(columnNames ...string) (*DataFrame, error) {
	newDf := NewDataFrame()
	for _, name := range columnNames {
		col, err := df.GetColumn(name)
		if err != nil {
			return nil, err
		}
		newDf.AddSeries(name, col)
	}
	return newDf, nil
}

// Filter devuelve un nuevo DataFrame con las filas que cumplen una condición
func (df *DataFrame) Filter(colName string, criteria func(interface{}) bool) *DataFrame {
	newDf := NewDataFrame()

	// 1. Encontrar la columna base para filtrar
	baseCol, ok := df.Columns[colName].([]interface{})
	if !ok {
		return newDf
	}

	// 2. Identificar los índices que cumplen la condición
	var indices []int
	for i, val := range baseCol {
		if criteria(val) {
			indices = append(indices, i)
		}
	}

	// 3. Crear las nuevas columnas solo con esos índices
	for name, col := range df.Columns {
		fullCol, ok := col.([]interface{})
		if !ok {
			continue
		}
		var filteredData []interface{}
		for _, idx := range indices {
			filteredData = append(filteredData, fullCol[idx])
		}
		newDf.AddSeries(name, filteredData)
	}

	return newDf
}

// ToJSON convierte el DataFrame en un JSON de lista de objetos (ideal para IA)
func (df *DataFrame) ToJSON() (string, error) {
	if len(df.Columns) == 0 {
		return "[]", nil
	}

	// Obtenemos los nombres de las columnas para iterar
	var headers []string
	for name := range df.Columns {
		headers = append(headers, name)
	}

	// Determinamos el número de filas basándonos en la primera columna
	firstCol, ok := df.Columns[headers[0]].([]interface{})
	if !ok {
		return "[]", nil
	}
	rowCount := len(firstCol)

	// Construimos una lista de mapas (cada mapa es una fila)
	rows := make([]map[string]interface{}, rowCount)
	for i := 0; i < rowCount; i++ {
		row := make(map[string]interface{})
		for _, name := range headers {
			col := df.Columns[name].([]interface{})
			// Seguridad por si las columnas tienen longitudes distintas
			if i < len(col) {
				row[name] = col[i]
			}
		}
		rows[i] = row
	}

	// Serializamos a JSON con indentación para que sea legible
	jsonData, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return "", err
	}

	return string(jsonData), nil
}