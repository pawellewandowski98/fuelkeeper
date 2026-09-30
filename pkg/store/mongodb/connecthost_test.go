package mongodb

import "testing"

// TestConnectHost_NeverLeaksCredentials reproduces the review finding that
// connectHost, when the URI's userinfo contains an unescaped special
// character, previously returned a fragment of the credentials instead of
// the host: net/url.Parse stops the authority at the first "/", so
// "mongodb://alice:1234/s3cretPW@localhost:27017/" (an unescaped "/" in the
// password) parsed to Host == "alice:1234" -- itself part of the
// username:password pair. connectHost must instead find the real host by
// taking the text after the LAST "@" anywhere in the URI and stopping at
// the next "/" or "?", and must never return a value containing the
// credentials.
//
// It also reproduces the follow-up review finding that an earlier fixed
// version still leaked credentials for an unescaped "?" in the password
// (it cut the search string at the first "?" before ever looking for "@",
// so "mongodb://alice:pa?ss1234@localhost:27017/" yielded host "alice:pa"):
// connectHost must instead refuse to guess a host at all -- falling back to
// the literal "mongodb" -- whenever a "?" appears anywhere before the last
// "@", since that ambiguity means it cannot tell a malformed userinfo "?"
// from an "@" that is actually inside the query string.
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
		{
			name: "unescaped ? in password (mongodb://) falls back rather than leaking a userinfo fragment",
			uri:  "mongodb://alice:pa?ss1234@localhost:27017/",
			want: "mongodb",
		},
		{
			name: "unescaped ? in password (mongodb+srv://) falls back rather than leaking a userinfo fragment",
			uri:  "mongodb+srv://alice:pa?ss1234@cluster0.example.net/db",
			want: "mongodb",
		},
		{
			name: "well-formed mongodb+srv credentials still resolve to the host",
			uri:  "mongodb+srv://user:pass@cluster0.example.net/db?retryWrites=true&w=majority",
			want: "cluster0.example.net",
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
