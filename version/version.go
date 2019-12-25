package version

import "encoding/json"

type Version struct {
	Branch     string
	Sha1       string
	LastCommit string
	Tag        string
}

func (v *Version) String() string {
	content, _ := json.Marshal(v)
	return string(content)
}

var version Version

func GetVersion() *Version {
	return &version
}
