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

func TestResolveMigrationUser(t *testing.T) {
	appDSN := "postgres://spool_app@localhost:5432/spool"
	adminDSN := "postgres://admin:secret@localhost:5432/spool"

	// 1. Explicit migrations user always takes precedence
	if u := resolveMigrationUser("explicit_admin", "app_user", adminDSN, appDSN); u != "explicit_admin" {
		t.Errorf("got %q, want explicit_admin", u)
	}

	// 2. Same DSN inherits appUser
	if u := resolveMigrationUser("", "app_user", appDSN, appDSN); u != "app_user" {
		t.Errorf("got %q, want app_user", u)
	}

	// 3. Different DSN does NOT inherit appUser (preserving embedded DSN user)
	if u := resolveMigrationUser("", "app_user", adminDSN, appDSN); u != "" {
		t.Errorf("got %q, want empty string", u)
	}
}

func TestVersion(t *testing.T) {
	if version != "0.3.0" {
		t.Errorf("version = %q, want 0.3.0", version)
	}
}
