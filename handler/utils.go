package handler

import (
	"net"
	"net/http"
	"strings"
)

// FromRequest return client's real public IP address from http request headers.
func FromRequest(r *http.Request) string {
	// Fetch header value
	xForwardedFor := r.Header.Get("J-Forwarded-For")
	// If both empty, return IP from remote address
	if  xForwardedFor == "" {
		var remoteIP string
		// If there are colon in remote address, remove the port number
		// otherwise, return remote address as is
		if strings.ContainsRune(r.RemoteAddr, ':') {
			remoteIP, _, _ = net.SplitHostPort(r.RemoteAddr)
		} else {
			remoteIP = r.RemoteAddr
		}
		return remoteIP
	}
	// Check list of IP in X-Forwarded-For and return the first global address
	for _, address := range strings.Split(xForwardedFor, ",") {
		address = strings.TrimSpace(address)
		return address
	}
	// If nothing succeed, return X-Real-IP
	return ""
}
