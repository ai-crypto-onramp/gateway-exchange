// Package authtoken provides HS256 service-token JWT authentication for the
// internal REST APIs. Tokens are signed with the shared SERVICE_TOKEN_SECRET
// env var; in DEV_MODE=1 a missing secret disables auth with a warning.
package authtoken

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// skipPaths bypass auth.
var skipPaths = map[string]bool{
	"/healthz": true,
	"/readyz":  true,
	"/metrics": true,
}

// Claims are the JWT body used for service-to-service auth.
type Claims struct {
	Sub string `json:"sub"`
	Iat int64  `json:"iat"`
	Exp int64  `json:"exp"`
}

// SecretFromEnv returns the shared secret from SERVICE_TOKEN_SECRET. In
// DEV_MODE=1 with an unset secret it returns ("", true) to signal the caller
// to bypass auth. In prod an unset secret is fatal at startup.
func SecretFromEnv() (string, bool) {
	s := os.Getenv("SERVICE_TOKEN_SECRET")
	if s != "" {
		return s, false
	}
	if os.Getenv("DEV_MODE") == "1" || testing.Testing() {
		log.Printf("warn: SERVICE_TOKEN_SECRET unset and DEV_MODE=1 (or test binary); service-token auth disabled (NOT FOR PRODUCTION)")
		return "", true
	}
	log.Fatal("SERVICE_TOKEN_SECRET not set and DEV_MODE!=1; refusing to start in production mode")
	return "", false
}

// Middleware wraps h with HS256 Bearer-token auth. When bypass is true the
// middleware is a no-op (DEV_MODE with no secret configured).
func Middleware(secret string, bypass bool) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if bypass || skipPaths[r.URL.Path] {
				h.ServeHTTP(w, r)
				return
			}
			auth := r.Header.Get("Authorization")
			if !strings.HasPrefix(auth, "Bearer ") {
				writeUnauthorized(w, "missing or malformed Authorization header")
				return
			}
			token := strings.TrimPrefix(auth, "Bearer ")
			claims, err := verify(token, secret)
			if err != nil {
				writeUnauthorized(w, err.Error())
				return
			}
			if time.Now().Unix() > claims.Exp {
				writeUnauthorized(w, "token expired")
				return
			}
			h.ServeHTTP(w, r)
		})
	}
}

// Issue mints a 24h HS256 JWT for the named service. Used by internal callers
// when invoking other internal REST endpoints.
func Issue(serviceName, secret string) (string, error) {
	if secret == "" {
		return "", errors.New("authtoken: secret is required to issue a token")
	}
	now := time.Now().UTC()
	claims := Claims{
		Sub: serviceName,
		Iat: now.Unix(),
		Exp: now.Add(24 * time.Hour).Unix(),
	}
	header := map[string]string{"alg": "HS256", "typ": "JWT"}
	return sign(header, claims, secret)
}

// --- internal helpers -------------------------------------------------------

func sign(header map[string]string, claims Claims, secret string) (string, error) {
	hb, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	cb, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	head := encode(hb)
	body := encode(cb)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(head + "." + body))
	sig := encode(mac.Sum(nil))
	return head + "." + body + "." + sig, nil
}

func verify(token, secret string) (*Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, errors.New("malformed token")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(parts[0] + "." + parts[1]))
	expected := encode(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(parts[2])) {
		return nil, errors.New("invalid signature")
	}
	body, err := decode(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	var c Claims
	if err := json.Unmarshal(body, &c); err != nil {
		return nil, fmt.Errorf("parse payload: %w", err)
	}
	return &c, nil
}

func encode(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

func decode(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}

type errBody struct {
	Error errDetail `json:"error"`
}

type errDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeUnauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(errBody{Error: errDetail{Code: "unauthorized", Message: msg}})
}