// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package filestore

import (
	"fmt"
	"net/url"
	"path/filepath"

	"strings"
)

type FileType string

type FileStoreType string

const (
	Memory     FileStoreType = "MEMORY"
	FileSystem FileStoreType = "LOCAL_FILESYSTEM"
	Azure      FileStoreType = "AZURE"
	S3         FileStoreType = "S3"
	GCS        FileStoreType = "GCS"
	HDFS       FileStoreType = "HDFS"
)

const (
	Parquet FileType = "parquet"
	CSV     FileType = "csv"
	DB      FileType = "db"
)

const (
	GSPrefix        = "gs://"
	S3Prefix        = "s3://"
	S3APrefix       = "s3a://"
	AzureBlobPrefix = "abfss://"
	HDFSPrefix      = "hdfs://"
)

var ValidSchemes = []string{
	GSPrefix, S3Prefix, S3APrefix, AzureBlobPrefix, HDFSPrefix,
}

func (ft FileType) Matches(file string) bool {
	ext := GetFileExtension(file)
	return FileType(ext) == ft
}

func IsValidFileType(file string) bool {
	for _, fileType := range []FileType{Parquet, CSV, DB} {
		if fileType.Matches(file) {
			return true
		}
	}
	return false
}

func GetFileExtension(file string) string {
	ext := filepath.Ext(file)
	return strings.ReplaceAll(ext, ".", "")
}

type Filepath interface {
	// Scheme encompasses
	// * protocol (e.g. s3://, gs://, abfss://)
	// * host (e.g. <container>@<account>.dfs.core.windows.net for Azure Blob)
	// * port (if applicable)
	// This naming technically conflicts with the standard definition of "scheme," which
	// is only the protocol _without_ the domain and port (i.e. authority); however, given
	// we're not using these components independently and there's no single term to denote
	// <SCHEME>://<HOST>:<PORT>, we're using "scheme" to encompass all three.
	Scheme() string
	SetScheme(scheme string) error

	// Returns the name of the bucket (S3) or container (Azure Blob Storage)
	Bucket() string
	SetBucket(bucket string) error

	// Returns the blob key, which is the relative path to the object (i.e. without the scheme or bucket/container)
	Key() string
	SetKey(key string) error

	// Returns the key prefix (i.e. the directory path to the object)
	KeyPrefix() string

	IsDir() bool
	SetIsDir(isDir bool)

	// Returns the file extension (e.g. "parquet", "csv", etc. of the object)
	Ext() FileType

	// Returns the full path to the object, including the scheme and bucket/container
	// TODO: rename to `ToURI`
	PathWithBucket() string
	// Consumes a URI (e.g. abfss://<container>@<storage_account>/path/to/file) and parses it into
	// the specific parts that the implementation expects.
	ParseFilePath(path string) error
	ParseDirPath(path string) error

	Validate() error
	IsValid() bool
}

func NewEmptyFilepath(storeType FileStoreType) (Filepath, error) {
	switch storeType {
	case S3:
		return &S3Filepath{FilePath{isDir: false}}, nil
	case Azure:
		return &AzureFilepath{}, nil
	case GCS:
		return &GCSFilepath{FilePath{isDir: false}}, nil
	case Memory:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	case FileSystem:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	//case DB:
	//	return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	case HDFS:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	default:
		return nil, fmt.Errorf("unknown store type '%s'", storeType)
	}
}

func NewEmptyDirpath(storeType FileStoreType) (Filepath, error) {
	switch storeType {
	case S3:
		return &S3Filepath{FilePath{isDir: true}}, nil
	case Azure:
		return &AzureFilepath{}, nil
	case GCS:
		return &GCSFilepath{FilePath{isDir: true}}, nil
	case Memory:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	case FileSystem:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	//case DB:
	//	return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	case HDFS:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	default:
		return nil, fmt.Errorf("unknown store type '%s'", storeType)
	}
}

type FilePath struct {
	scheme  string
	bucket  string
	key     string
	isDir   bool
	isValid bool
}

func (fp *FilePath) SetScheme(scheme string) error {
	if err := fp.checkSchemes(scheme); err != nil {
		return err
	}
	fp.scheme = scheme
	return nil
}

func (fp *FilePath) Scheme() string {
	return fp.scheme
}

func (fp *FilePath) SetBucket(bucket string) error {
	if bucket == "" {
		return fmt.Errorf("bucket cannot be empty")
	}
	fp.bucket = bucket
	return nil
}

func (fp *FilePath) Bucket() string {
	return fp.bucket
}

func (fp *FilePath) SetKey(key string) error {
	fp.key = strings.Trim(key, "/")
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	return nil
}

func (fp *FilePath) Key() string {
	return fp.key
}

func (fp *FilePath) KeyPrefix() string {
	return filepath.Dir(fp.key)
}

func (fp *FilePath) Ext() FileType {
	ext := filepath.Ext(fp.key)
	// filepath.Ext returns the extension with the "." prefix, so we need to trim it
	// to match our FileType type.
	return FileType(strings.TrimPrefix(ext, "."))
}

func (fp *FilePath) PathWithBucket() string {
	return fmt.Sprintf("%s%s/%s", fp.scheme, fp.bucket, fp.key)
}

func (fp *FilePath) SetIsDir(isDir bool) {
	fp.isDir = isDir
}

func (fp *FilePath) IsDir() bool {
	return fp.isDir
}

func (fp *FilePath) ParseFilePath(fullPath string) error {
	err := fp.parsePath(fullPath)
	if err != nil {
		return fmt.Errorf("file: %v", err)
	}
	fp.isDir = false
	return nil
}

func (fp *FilePath) ParseDirPath(fullPath string) error {
	err := fp.parsePath(fullPath)
	if err != nil {
		return fmt.Errorf("dir: %v", err)
	}
	// To ensure consistency, we check to see if the last element has an extension, and if so,
	// we remove it to ensure we're always dealing with a directory path.
	lastElem := filepath.Base(fp.key)
	if filepath.Ext(lastElem) != "" {
		fp.key = filepath.Dir(fp.key)
	}
	fp.isDir = true
	return nil
}

func (fp *FilePath) checkSchemes(scheme string) error {
	for _, s := range ValidSchemes {
		if s == scheme {
			return nil
		}
	}
	return fmt.Errorf("invalid scheme '%s', must be one of %v", scheme, ValidSchemes)
}

func (fp *FilePath) parsePath(fullPath string) error {
	// Parse the URI into a url.URL object.
	u, err := url.Parse(fullPath)
	if err != nil {
		return fmt.Errorf("could not parse full path '%s': %v", fullPath, err)
	}
	// Extract the bucket and path components from the URI.
	bucket := u.Host
	path := strings.TrimPrefix(u.Path, "/")
	// url.Parse returns the scheme without the "://" suffix, so we need to add it back
	// to ensure comparison with our hardcoded schemes works, as well as building the
	// absolute path.
	scheme := fmt.Sprintf("%s://", u.Scheme)
	err = fp.checkSchemes(scheme)
	if err != nil {
		return err
	} else {
		fp.scheme = scheme
	}

	fp.bucket = bucket
	fp.key = path
	return nil
}

func (fp *FilePath) IsValid() bool {
	return fp.isValid
}

func (fp *FilePath) Validate() error {
	return fmt.Errorf("not implemented")
}

type S3Filepath struct {
	FilePath
}

func (s3 *S3Filepath) Validate() error {
	if s3.scheme != "s3://" && s3.scheme != "s3a://" {
		return fmt.Errorf("invalid scheme '%s', must be 's3:// or 's3a://'", s3.scheme)
	}
	if s3.bucket == "" {
		return fmt.Errorf("bucket cannot be empty")
	} else {
		s3.bucket = strings.Trim(s3.bucket, "/")
	}
	if s3.key == "" || s3.key == "/" {
		return fmt.Errorf("key cannot be empty")
	} else {
		s3.key = strings.Trim(s3.key, "/")
	}

	s3.isValid = true
	return nil
}

func (s3 *S3Filepath) PathWithBucket() string {
	return fmt.Sprintf("%s%s/%s", s3.scheme, s3.bucket, s3.key)
}

type AzureFilepath struct {
	StorageAccount string
	FilePath
}

func (azure *AzureFilepath) PathWithBucket() string {
	return fmt.Sprintf("%s%s@%s.dfs.core.windows.net/%s", azure.scheme, azure.bucket, azure.StorageAccount, azure.key)
}

// **NOTE**: Due to Azure Blob Storage's unique URI format, we need to re-implement this method
// on the derived type to ensure we can properly handle the `bucket` field.
func (azure *AzureFilepath) ParseFilePath(fullPath string) error {
	u, err := url.Parse(fullPath)
	if err != nil {
		return fmt.Errorf("could not parse full path '%s': %v", fullPath, err)
	}
	// Our scheme is the protocol + "://", so we need to suffix the scheme with "://"
	// to ensure the comparison works.
	scheme := fmt.Sprintf("%s://", u.Scheme)
	err = azure.FilePath.checkSchemes(scheme)
	if err != nil {
		return err
	}
	azure.FilePath.scheme = scheme
	azure.FilePath.bucket = u.User.String()              // The container will be in the User field due to the format <scheme>://<container>@<storage_account>
	azure.StorageAccount = strings.Split(u.Host, ".")[0] // The host will be in the format <storage_account>.dfs.core.windows.net
	azure.FilePath.key = strings.TrimPrefix(u.Path, "/")
	azure.FilePath.isDir = false
	return nil
}

func (azure *AzureFilepath) ParseDirPath(fullPath string) error {
	err := azure.ParseFilePath(fullPath)
	if err != nil {
		return err
	}
	azure.FilePath.isDir = true
	lastElem := filepath.Base(azure.FilePath.key)
	if filepath.Ext(lastElem) != "" {
		azure.FilePath.key = filepath.Dir(azure.FilePath.key)
	}
	return nil
}

func (azure *AzureFilepath) Validate() error {
	if azure.scheme != "abfss://" {
		return fmt.Errorf("invalid scheme '%s', must be 'abfss://'", azure.scheme)
	}
	if azure.StorageAccount == "" {
		return fmt.Errorf("storage account cannot be empty")
	}
	if azure.bucket == "" {
		return fmt.Errorf("bucket cannot be empty")
	} else {
		azure.bucket = strings.Trim(azure.bucket, "/")
	}
	if azure.key == "" {
		return fmt.Errorf("key cannot be empty")
	} else {
		azure.key = strings.Trim(azure.key, "/")
	}
	azure.isValid = true
	return nil
}

type GCSFilepath struct {
	FilePath
}

func (gcs *GCSFilepath) PathWithBucket() string {
	return fmt.Sprintf("%s%s/%s", gcs.scheme, gcs.bucket, gcs.key)
}

func (gcs *GCSFilepath) Validate() error {
	if gcs.scheme != "gs://" {
		return fmt.Errorf("invalid scheme '%s', must be 'gs://'", gcs.scheme)
	}
	if gcs.bucket == "" {
		return fmt.Errorf("bucket cannot be empty")
	} else {
		gcs.bucket = strings.Trim(gcs.bucket, "/")
	}
	if gcs.key == "" {
		return fmt.Errorf("key cannot be empty")
	} else {
		gcs.key = strings.Trim(gcs.key, "/")
	}
	gcs.isValid = true
	return nil
}

type HDFSFilepath struct {
	FilePath
}

func (hdfs *HDFSFilepath) Validate() error {
	return fmt.Errorf("not implemented")
}

type LocalFilepath struct {
	FilePath
}
