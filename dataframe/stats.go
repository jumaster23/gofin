package dataframe

func (df *DataFrame) Sum(colName string) float64 {
	col, _ := df.GetColumn(colName)
	data := col.([]interface{})
	var total float64
	for _, v := range data {
		if val, ok := v.(float64); ok {
			total += val
		}
	}
	return total
}