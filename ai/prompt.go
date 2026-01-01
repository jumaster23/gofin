package ai

import (
	"fmt"
)

// PrepareFinancialPrompt toma datos y crea un texto para Gemini
func PrepareFinancialPrompt(resumen string, datos []float64) string {
	return fmt.Sprintf(
		"Analiza estos movimientos financieros: %v. Contexto: %s. Dame consejos de ahorro.",
		datos, 
		resumen,
	)
}