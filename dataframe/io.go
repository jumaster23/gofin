package dataframe

import (
	"fmt"
	"strconv"

	"github.com/xuri/excelize/v2"
)

// ReadExcelRange lee desde una celda de inicio hasta una de fin para definir las columnas
// y luego lee todas las filas hacia abajo automáticamente.
func ReadExcelRange(filePath, sheetName, startCell, endCell string) (*DataFrame, error) {
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Convertir coordenadas (ej: "A5" -> Col 1, Fila 5)
	startCol, startRow, _ := excelize.CellNameToCoordinates(startCell)
	endCol, _, _ := excelize.CellNameToCoordinates(endCell)

	rows, err := f.GetRows(sheetName)
	if err != nil {
		return nil, err
	}

	df := NewDataFrame()
	tempData := make(map[string][]interface{})
	var headers []string

	// 1. Identificar Headers (Fila startRow)
	if len(rows) < startRow {
		return nil, fmt.Errorf("el archivo no tiene tantas filas")
	}
	
	headerRow := rows[startRow-1]
	for j := startCol - 1; j < endCol; j++ {
		name := ""
		if j < len(headerRow) && headerRow[j] != "" {
			name = headerRow[j]
		} else {
			name = fmt.Sprintf("Col_%d", j+1) // Nombre genérico si está vacío
		}
		headers = append(headers, name)
	}

	// 2. Leer datos hacia abajo
	for i := startRow; i < len(rows); i++ {
		row := rows[i]
		
		// Si la fila está totalmente vacía en el rango de interés, paramos (como Polars)
		hasData := false
		for j := startCol - 1; j < endCol && j < len(row); j++ {
			if row[j] != "" {
				hasData = true
				break
			}
		}
		if !hasData { break }

		// Mapear cada celda a su header
		for j := startCol - 1; j < endCol; j++ {
			headerName := headers[j-(startCol-1)]
			var value interface{} = "" // Default vacío

			if j < len(row) {
				cellValue := row[j]
				// Intentar convertir a número para cálculos financieros
				if val, err := strconv.ParseFloat(cellValue, 64); err == nil {
					value = val
				} else {
					value = cellValue
				}
			}
			tempData[headerName] = append(tempData[headerName], value)
		}
	}

	for _, name := range headers {
		df.AddSeries(name, tempData[name])
	}

	return df, nil
}