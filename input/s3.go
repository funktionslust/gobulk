package input

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/funktionslust/gobulk"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/zap"
)

// S3InputConfig represents the S3Input configurable fields model.
type S3InputConfig struct {
	AwsCfg        *aws.Config
	Bucket        string `validate:"required"`
	Prefix        string
	ScanIntervals string
	ScanThrottle  int
	Delimiter     string
	EncodingType  string
	MaxKeys       int64 `validate:"lte=1000"` // AWS API allows to receive not more than 1000 items in a call
}

type (
	// InputOption is the base form of an option parameter for S3Input.
	InputOption func(i *S3Input)
	// ModifyScan is the form of the func to be used as an optional parameter for the S3Input that
	// is called between getting list of objects from S3 and sending them to the contCh. Could be
	// used to filter result objects or do other needed stuff.
	ModifyScan func(output *s3.ListObjectsOutput) *s3.ListObjectsOutput
)

// WithModifyScan enhanced the S3Input with the passed modifyScan.
func WithModifyScan(modifyScan ModifyScan) InputOption {
	return func(i *S3Input) {
		i.modifyScan = modifyScan
	}
}

// NewS3Input returns a new instance of the S3Input.
func NewS3Input(cfg S3InputConfig, opts ...InputOption) *S3Input {
	i := &S3Input{
		Cfg:        cfg,
		modifyScan: defaultModifyScan,
	}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

// S3Input represents an input that reads files from an AWS S3 Bucket.
type S3Input struct {
	gobulk.BaseStorage
	Cfg        S3InputConfig
	modifyScan ModifyScan
}

// Setup contains the storage preparations like connection etc. Is called only once at the very
// beginning of the work with the storage. As for the S3Input, it checks whether the config for
// the input is proper by connecting and performing a simple S3 API call.
func (i *S3Input) Setup() error {
	sess, err := session.NewSession(i.Cfg.AwsCfg)
	if err != nil {
		return fmt.Errorf("failed to create a new s3 session: %v", err)
	}
	svc := s3.New(sess)
	err = svc.ListObjectsPages(i.buildListObjectsInput(""), func(p *s3.ListObjectsOutput, lastPage bool) bool { return false })
	if err != nil {
		return fmt.Errorf("ping s3 query error: %v", err)
	}
	return nil
}

// Scan scans the S3 bucket for new containers and sends them to the channel. It starts the scan with
// the container defined as marker and stops either on an S3-interaction error, or when the context
// is cancelled, or when all the input containers are read.
func (i *S3Input) Scan(ctx context.Context, marker *gobulk.Container, contCh chan<- []*gobulk.Container, doneCh chan<- struct{}, errCh chan<- error) {
	intervals := make([]int, 0)
	if i.Cfg.ScanIntervals != "" {
		for _, interval := range strings.Split(i.Cfg.ScanIntervals, ",") {
			intInterval, err := strconv.Atoi(interval)
			if err != nil {
				errCh <- fmt.Errorf("invalid ScanIntervals: all entires should be numbers")
				return
			}
			intervals = append(intervals, intInterval)
		}
	}
	sess, err := session.NewSession(i.Cfg.AwsCfg)
	if err != nil {
		errCh <- err
		return
	}
	svc := s3.New(sess)
	throttle := i.Cfg.ScanThrottle
	var m string
	if marker != nil {
		m = i.joinPath(marker.InputRepository, marker.InputIdentifier)
	}
	listInput := i.buildListObjectsInput(m)
	if listInput.Marker == nil {
		i.Logger.Info("running S3Input Scan from scratch")
	} else {
		i.Logger.Info("running S3Input Scan using a marker", zap.String("marker", *listInput.Marker))
	}
	if err = svc.ListObjectsPages(listInput, func(p *s3.ListObjectsOutput, lastPage bool) bool { // this iterates over all pages of the bucket(prefix). so it can take very long
		containers := []*gobulk.Container{}
		p = i.modifyScan(p)
		for _, obj := range p.Contents {
			container := i.s3ObjectToContainer(obj)
			containers = append(containers, container)
		}
		select {
		case <-ctx.Done():
			return false
		default:
			if len(containers) != 0 {
				select {
				case contCh <- containers:
					i.Logger.Info("a bulk of containers has been scanned",
						zap.Int("amount", len(containers)),
						zap.String("first_container_identifier", containers[0].InputIdentifier),
						zap.String("last_container_identifier", containers[len(containers)-1].InputIdentifier),
					)
				case <-ctx.Done():
					return false
				}
			}
		}
		if throttle != 0 {
			time.Sleep(time.Duration(throttle) * time.Second)
		}
		return !lastPage // false means stop => if we reached lastPage==true we stop by returning false
	}); err != nil {
		select {
		case <-ctx.Done():
		case errCh <- err:
		}
		return
	}
	select {
	case <-ctx.Done():
	case doneCh <- struct{}{}:
	}
}

// Read reads the raw data of a container.
func (i *S3Input) Read(container *gobulk.Container) (map[string][]byte, error) {
	// TODO always new session? bad idea? maybe reuse it?
	sess, err := session.NewSession(i.Cfg.AwsCfg)
	if err != nil {
		return nil, gobulk.NewIssue(fmt.Errorf("failed to establish an S3 connection: %v", err), "", gobulk.IssueTypeInfrastructure, "")
	}
	objectPaths := []string{}
	if strings.HasSuffix(container.InputIdentifier, "/") {
		svc := s3.New(sess)
		folderPath := aws.String(i.joinPath(container.InputRepository, container.InputIdentifier))
		inputParams := &s3.ListObjectsInput{
			Bucket: aws.String(i.Cfg.Bucket),
			Prefix: folderPath,
			Marker: folderPath,
		}
		if err = svc.ListObjectsPages(inputParams, func(p *s3.ListObjectsOutput, lastPage bool) bool {
			for _, obj := range p.Contents {
				objectPaths = append(objectPaths, *obj.Key)
			}
			return !lastPage
		}); err != nil {
			return nil, gobulk.NewIssue(fmt.Errorf("failed retrieve S3 object pages: %v", err), "", gobulk.IssueTypeInfrastructure, "")
		}
	} else {
		objectPaths = append(objectPaths, path.Join(container.InputRepository, container.InputIdentifier))
	}
	files := make(map[string][]byte, len(objectPaths))
	for _, objectPath := range objectPaths {
		downloader := s3manager.NewDownloader(sess)
		obj := &s3.GetObjectInput{
			Bucket: aws.String(i.Cfg.Bucket),
			Key:    aws.String(objectPath),
		}
		buff := &aws.WriteAtBuffer{}
		_, err = downloader.Download(buff, obj)
		if err != nil {
			return nil, gobulk.NewIssue(fmt.Errorf("failed to download a container data from S3: %v", err), "", gobulk.IssueTypeInfrastructure, "")
		}
		files[objectPath] = buff.Bytes()
	}
	return files, nil
}

// s3ObjectToContainer maps an S3 file to a bulk container.
func (i *S3Input) s3ObjectToContainer(obj *s3.Object) *gobulk.Container {
	contentHash := ""
	if obj.ETag != nil { // this fixes the following S3 API bug: https://github.com/aws/aws-sdk-net/issues/815
		contentHash = strings.ReplaceAll(*obj.ETag, "\"", "")
	}
	key := *obj.Key
	var repo string
	var identifier string
	if strings.HasSuffix(key, "/") {
		key = strings.TrimSuffix(key, "/")
		repo = path.Dir(key)
		identifier = path.Base(key) + "/"
	} else {
		repo = path.Dir(key)
		identifier = path.Base(key)
	}
	return &gobulk.Container{
		Created:         time.Now(),
		Started:         nil,
		Finished:        nil,
		InputRepository: repo,
		InputIdentifier: identifier,
		Size:            uint64(*obj.Size),
		LastModified:    *obj.LastModified,
		ContentHash:     contentHash,
		ProcessID:       i.ProcessID,
	}
}

// buildListObjectsInput builds an input for S3 ListObjects queries based on input config and passed
// marker. If marker is an empty string, no marker is specified in the result value.
func (i *S3Input) buildListObjectsInput(marker string) *s3.ListObjectsInput {
	var reqEncodingType *string
	if i.Cfg.EncodingType != "" {
		reqEncodingType = aws.String(i.Cfg.EncodingType)
	}
	var reqMarker *string
	if marker != "" {
		reqMarker = aws.String(marker)
	}
	var reqDelimiter *string
	if i.Cfg.Delimiter != "" {
		reqDelimiter = aws.String(i.Cfg.Delimiter)
	}
	var reqMaxKeys *int64
	if i.Cfg.MaxKeys != 0 {
		reqMaxKeys = aws.Int64(i.Cfg.MaxKeys)
	}
	return &s3.ListObjectsInput{
		Bucket:       aws.String(i.Cfg.Bucket),
		Prefix:       aws.String(i.Cfg.Prefix),
		EncodingType: reqEncodingType,
		Marker:       reqMarker,
		Delimiter:    reqDelimiter,
		MaxKeys:      reqMaxKeys,
	}
}

// joinPath works as the path.Join func but keeps the trailing slash. It's urgent for S3 markers to
// have the slash so S3 knows it's a directory.
func (S3Input) joinPath(elem ...string) string {
	p := path.Join(elem...)
	if strings.HasSuffix(elem[len(elem)-1], "/") {
		p += "/"
	}
	return p
}

// defaultModifyScan simply returns the initial output.
var defaultModifyScan = func(output *s3.ListObjectsOutput) *s3.ListObjectsOutput {
	return output
}
