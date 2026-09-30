package mongodb

import "testing"

// TestConnectHost_NeverLeaksCredentials reproduces the review finding that
// connectHost, when the URI's userinfo contains an unescaped special
// character, previously returned a fragment of the credentials instead of
// the host: net/url.Parse stops the authority at the first "/", so
// "mongodb://alice:1234/s3cretPW@localhost:27017/" (an unescaped "/" in the
// password) parsed to Host == "alice:1234" -- itself part of the
// username:password pair. connectHost must instead find the real host by
// cutting at the LAST "@" before any "?" and stopping at the next "/" or
// "?", and must never return a value containing the credentials.
func TestConnectHost_NeverLeaksCredentials(t *testing.T) {
	cases := []struct {
		name string
		uri  string
		want string
	}{
		{
			name: "unescaped slash in password",
			uri:  "mongodb://alice:1234/s3cretPW@localhost:27017/",
			want: "localhost:27017",
		},
		{
			name: "unescaped @ in password (user:p@ss@host style)",
			uri:  "mongodb://user:p@ss@host:27017/db",
			want: "host:27017",
		},
		{
			name: "well-formed credentials still resolve to the host",
			uri:  "mongodb://user:pass@host1:27017,host2:27018/db?replicaSet=rs0",
			want: "host1:27017,host2:27018",
		},
		{
			name: "no credentials at all",
			uri:  "mongodb://localhost:27017/db",
			want: "localhost:27017",
		},
		{
			name: "no host-like substring falls back to the literal label",
			uri:  "mongodb://",
			want: "mongodb",
		},
		{
			name: "unparseable scheme falls back to the literal label",
			uri:  "not-a-uri",
			want: "mongodb",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := connectHost(tc.uri)
			if got != tc.want {
				t.Fatalf("connectHost(%q) = %q, want %q", tc.uri, got, tc.want)
			}
		})
	}
}
