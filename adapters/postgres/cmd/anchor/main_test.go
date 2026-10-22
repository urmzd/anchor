package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// cli runs the command line with env as the whole environment.
func cli(t *testing.T, env map[string]string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errb bytes.Buffer
	code = execute(context.Background(), args, func(k string) string { return env[k] }, &out, &errb)
	return code, out.String(), errb.String()
}

// A closed local port: connecting fails at once, with no database needed.
const (
	secret    = "s3cret-Passw0rd"
	closedURL = "postgres://ledger:" + secret + "@127.0.0.1:1/ledger?sslmode=disable&connect_timeout=2"
)

func TestUsage(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
		want string
	}{
		{nil, exitUsage, "Usage:"},
		{[]string{"bogus"}, exitUsage, `unknown command "bogus"`},
		{[]string{"runs", "-nope"}, exitUsage, "flag provided but not defined"},
		{[]string{"runs", "-db", closedURL, "extra"}, exitUsage, "runs takes no arguments"},
		{[]string{"runs", "-db", closedURL, "-status", "running"}, exitUsage, "want pending, succeeded or failed"},
		{[]string{"runs", "-db", closedURL, "-limit", "0"}, exitUsage, "want a positive number"},
		{[]string{"show", "-db", closedURL}, exitUsage, "show takes one RUN argument"},
		{[]string{"show", "-db", closedURL, "a", "b"}, exitUsage, "show takes one RUN argument"},
		{[]string{"replay", "-db", closedURL}, exitUsage, "replay takes one RUN argument"},
		{[]string{"replay", "-db", closedURL, "-codec", "", "r"}, exitUsage, "-codec"},
		{[]string{"show", "-db", closedURL, "-runs-table", "Bad Name", "r"}, exitUsage, "Mapping.Runs.Table"},
	} {
		code, out, errOut := cli(t, nil, tc.args...)
		if code != tc.code || !strings.Contains(out+errOut, tc.want) {
			t.Errorf("anchor %q = %d, stdout %q, stderr %q; want %d mentioning %q", tc.args, code, out, errOut, tc.code, tc.want)
		}
		if strings.Contains(out+errOut, secret) {
			t.Errorf("anchor %q printed the database password:\n%s%s", tc.args, out, errOut)
		}
	}
}

func TestHelp(t *testing.T) {
	env := map[string]string{envDatabaseURL: closedURL}
	for _, args := range [][]string{{"help"}, {"-h"}, {"runs", "-h"}, {"show", "-help"}, {"replay", "-h"}} {
		code, out, errOut := cli(t, env, args...)
		if code != exitOK {
			t.Errorf("anchor %q = %d, want 0", args, code)
		}
		if !strings.Contains(out+errOut, "Usage") {
			t.Errorf("anchor %q printed no usage: %q %q", args, out, errOut)
		}
		// The -db default stays empty, so help never shows the URL.
		if strings.Contains(out+errOut, secret) || strings.Contains(out+errOut, "127.0.0.1:1") {
			t.Errorf("anchor %q printed the database URL:\n%s%s", args, out, errOut)
		}
	}
}

func TestDatabaseURLRequired(t *testing.T) {
	code, _, errOut := cli(t, nil, "runs")
	if code != exitUsage || !strings.Contains(errOut, envDatabaseURL) {
		t.Fatalf("runs without a URL = %d, %q; want usage naming %s", code, errOut, envDatabaseURL)
	}
}

// TestDatabaseURLNeverPrinted: failures to parse or to connect say why
// without echoing the URL or its password, whether it came from -db or
// from the environment.
func TestDatabaseURLNeverPrinted(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		args []string
		code int
		want string
	}{
		{"unparsable flag", nil, []string{"runs", "-db", "postgres://ledger:" + secret + "@[::1"}, exitUsage, "does not parse"},
		{"unparsable env", map[string]string{envDatabaseURL: "postgres://ledger:" + secret + "@host:notaport/x"}, []string{"runs"}, exitUsage, "does not parse"},
		{"unreachable flag", nil, []string{"show", "-db", closedURL, "r"}, exitFailed, "connect to the database"},
		{"unreachable env", map[string]string{envDatabaseURL: closedURL}, []string{"replay", "r"}, exitFailed, "connect to the database"},
		// -db wins over the environment: the environment's URL would not
		// even parse.
		{"flag over env", map[string]string{envDatabaseURL: "::not a url::"}, []string{"runs", "-db", closedURL}, exitFailed, "connect to the database"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errOut := cli(t, tc.env, tc.args...)
			if code != tc.code || !strings.Contains(errOut, tc.want) {
				t.Fatalf("= %d, stdout %q, stderr %q; want %d mentioning %q", code, out, errOut, tc.code, tc.want)
			}
			for _, leak := range []string{secret, "postgres://", "user=ledger", "database=ledger"} {
				if strings.Contains(out+errOut, leak) {
					t.Fatalf("output leaks %q:\n%s%s", leak, out, errOut)
				}
			}
		})
	}
}

func TestFlagsAfterTheRunArgument(t *testing.T) {
	var stderr bytes.Buffer
	o, rest, err := parseFlags("show", []string{"order-1", "-json", "-db", "x"}, &stderr)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if len(rest) != 1 || rest[0] != "order-1" || !o.json || o.db != "x" {
		t.Fatalf("parseFlags = %+v, %q; want RUN order-1 with -json and -db", o, rest)
	}
}

func TestScrub(t *testing.T) {
	dsn := "postgres://ledger:" + secret + "@db.internal:5432/ledger"
	msg := scrub("boom "+dsn+" and :"+secret+"@ and password="+secret, dsn)
	if strings.Contains(msg, secret) || strings.Contains(msg, "db.internal") {
		t.Fatalf("scrub left the URL or password: %q", msg)
	}
	kv := "host=db.internal user=ledger password=" + secret
	if msg := scrub("failed: password="+secret, kv); strings.Contains(msg, secret) {
		t.Fatalf("scrub left a keyword/value password: %q", msg)
	}
}

func TestPoolConfigIsReadOnly(t *testing.T) {
	cfg, err := poolConfig(closedURL)
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}
	if got := cfg.ConnConfig.RuntimeParams["default_transaction_read_only"]; got != "on" {
		t.Fatalf("default_transaction_read_only = %q, want on", got)
	}
	if got := cfg.ConnConfig.RuntimeParams["application_name"]; got != "anchor-cli" {
		t.Fatalf("application_name = %q, want anchor-cli", got)
	}
	cfg, err = poolConfig(closedURL + "&application_name=ops&default_transaction_read_only=off")
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}
	if got := cfg.ConnConfig.RuntimeParams["default_transaction_read_only"]; got != "on" {
		t.Fatalf("a URL turned read-only off: %q", got)
	}
	if got := cfg.ConnConfig.RuntimeParams["application_name"]; got != "ops" {
		t.Fatalf("application_name = %q, want the URL's ops", got)
	}
}

func TestTextPayload(t *testing.T) {
	for _, tc := range []struct {
		in, want string
	}{
		{"", "-"},
		{"{ \"a\" : 1 }", `{"a":1}`},
		{"plain\ttext", `"plain\ttext"`},
		{"\xff\xfe", "base64://4="},
	} {
		if got := textPayload([]byte(tc.in)); got != tc.want {
			t.Errorf("textPayload(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	long := textPayload([]byte(`"` + strings.Repeat("x", 500) + `"`))
	if !strings.HasSuffix(long, "... (502 bytes)") || len(long) > textWidth+20 {
		t.Errorf("long payload = %q, want it clipped with its size", long)
	}
}

func TestCellEscapesControlCharacters(t *testing.T) {
	if got := cell("a\nb\x1b[31m"); got != `a\nb\x1b[31m` {
		t.Fatalf("cell = %q", got)
	}
	if got := cell("plain-key#0"); got != "plain-key#0" {
		t.Fatalf("cell = %q", got)
	}
}
