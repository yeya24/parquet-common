package schema

import (
	"fmt"
	"strings"
)

const (
	LabelColumnPrefix = "l_"
	DataColumnPrefix  = "s_data_"

	DataColSizeMd = "data_col_duration_ms"
)

func LabelToColumn(lbl string) string {
	return fmt.Sprintf("%s%s", LabelColumnPrefix, lbl)
}

func ExtractLabelFromColumn(col string) (string, bool) {
	if !strings.HasPrefix(col, LabelColumnPrefix) {
		return "", false
	}

	return strings.TrimPrefix(col, LabelColumnPrefix), true
}

func IsDataColumn(col string) bool {
	return strings.HasPrefix(col, DataColumnPrefix)
}

func DataColumn(i int) string {
	return fmt.Sprintf("%s%v", DataColumnPrefix, i)
}
