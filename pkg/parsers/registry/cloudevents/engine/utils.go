package engine

import (
	"net/url"
	"regexp"
	"strconv"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

var extractSubjectAndVersion = regexp.MustCompile(`.*/schemas/ids/(.*)`)

func extractSchemaIDAndURL(in string) (hostPort string, schemaID uint32, err error) {
	u, err := url.Parse(in)
	if err != nil {
		logger.Log.Error("extractSchemaIDAndURL: invalid URL", log.Error(err))
		return "", 0, xerrors.Errorf("parse url: %w", err)
	}

	hostPort = u.Scheme + "://" + u.Hostname()
	port := u.Port()
	if port != "" {
		hostPort += ":" + port
	}

	arr := extractSubjectAndVersion.FindStringSubmatch(in)
	if len(arr) != 2 {
		return "", 0, xerrors.Errorf("unable to find subject & version into URL, url:%s", in)
	}

	schemaIDInt, err := strconv.Atoi(arr[1])
	if err != nil {
		return "", 0, xerrors.Errorf("unable to convert version string to int, url:%s", in)
	}

	return hostPort, uint32(schemaIDInt), nil
}
