package main

import (
	"testing"
)

func TestIsAzureAuth(t *testing.T) {
	positiveCases := []string{
		"azure",
		"AZURE",
		"Azure",
		"azure-identity",
		"azure-ad",
		"azure_ad",
		"entra",
		"ENTRA",
		"workload-identity",
	}

	for _, c := range positiveCases {
		if !isAzureAuth(c) {
			t.Errorf("isAzureAuth(%q) = false, want true", c)
		}
	}

	negativeCases := []string{
		"password",
		"PASSWORD",
		"",
		"basic",
		"cert",
	}

	for _, c := range negativeCases {
		if isAzureAuth(c) {
			t.Errorf("isAzureAuth(%q) = true, want false", c)
		}
	}
}

func TestHasPasswordInDSN(t *testing.T) {
	withPassword := []string{
		"postgres://user:secret@localhost:5432/spool?sslmode=disable",
		"host=localhost user=admin password=secret dbname=spool",
	}
	for _, dsn := range withPassword {
		if !hasPasswordInDSN(dsn) {
			t.Errorf("hasPasswordInDSN(%q) = false, want true", dsn)
		}
	}

	withoutPassword := []string{
		"postgres://id-ixs-rng-dev-em20-spl-02@localhost:5432/spool?sslmode=disable",
		"host=localhost user=id-ixs-rng-dev-em20-spl-02 dbname=spool sslmode=require",
		"invalid dsn %%%",
	}
	for _, dsn := range withoutPassword {
		if hasPasswordInDSN(dsn) {
			t.Errorf("hasPasswordInDSN(%q) = true, want false", dsn)
		}
	}
}
