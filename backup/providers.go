package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	filestore "github.com/featureform/filestore"
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	"gocloud.dev/gcp"
)

type Provider interface {
	Init() error
	// **NOTE:** Upload accepts strings instead of filestore.Filepath because
	// the store, which could create a filestore-specific filepath, is not exposed
	// by the Provider interface, and it seems unnecessary to expose it at this time.
	// In contrast, Download accepts a filestore.Filepath because the LatestBackupName
	// returns a filestore.Filepath and dest is an instance of filestore.LocalFilepath.
	Upload(src, dest string) error
	Download(src, dest filestore.Filepath) error
	LatestBackupName(filenamePrefix string) (filestore.Filepath, error)
}

type Azure struct {
	AccountName   string
	AccountKey    string
	ContainerName string
	Path          string
	store         provider.FileStore
}

func (az *Azure) Init() error {
	filestoreConfig := &pc.AzureFileStoreConfig{
		AccountName:   az.AccountName,
		AccountKey:    az.AccountKey,
		ContainerName: az.ContainerName,
		Path:          az.Path,
	}
	config, err := filestoreConfig.Serialize()
	if err != nil {
		return fmt.Errorf("cannot serialize the AzureFileStoreConfig: %v", err)
	}

	filestore, err := provider.NewAzureFileStore(config)
	if err != nil {
		return fmt.Errorf("cannot create Azure Filestore: %v", err)
	}
	az.store = filestore
	return nil
}

func (az *Azure) Upload(src, dest string) error {
	source := &filestore.LocalFilepath{}
	if err := source.SetKey(src); err != nil {
		return fmt.Errorf("cannot set source key: %v", err)
	}
	destination, err := az.store.CreateFilePath(dest, false)
	if err != nil {
		return fmt.Errorf("cannot create destination file path: %v", err)
	}
	return az.store.Upload(source, destination)
}

func (az *Azure) Download(src, dest filestore.Filepath) error {
	return az.store.Download(src, dest)
}

func (az *Azure) LatestBackupName(dir string) (filestore.Filepath, error) {
	dirPath, err := az.store.CreateFilePath(dir, true)
	if err != nil {
		return nil, fmt.Errorf("cannot create dir path: %v", err)
	}
	return az.store.NewestFileOfType(dirPath, filestore.DB)
}

type S3 struct {
	AWSAccessKeyId string
	AWSSecretKey   string
	BucketRegion   string
	BucketName     string
	BucketPath     string
	store          provider.FileStore
}

func (s3 *S3) Init() error {
	filestoreConfig := pc.S3FileStoreConfig{
		Credentials: pc.AWSCredentials{
			AWSAccessKeyId: s3.AWSAccessKeyId,
			AWSSecretKey:   s3.AWSSecretKey,
		},
		BucketRegion: s3.BucketRegion,
		BucketPath:   s3.BucketName,
		Path:         s3.BucketPath,
	}

	config, err := filestoreConfig.Serialize()
	if err != nil {
		return fmt.Errorf("cannot serialize S3 Config: %v", err)
	}

	filestore, err := provider.NewS3FileStore(config)
	if err != nil {
		return fmt.Errorf("cannot create S3 Filestore: %v", err)
	}
	s3.store = filestore
	return nil
}

func (s3 *S3) Upload(src, dest string) error {
	source := &filestore.LocalFilepath{}
	if err := source.SetKey(src); err != nil {
		return fmt.Errorf("cannot set source key: %v", err)
	}
	destination, err := s3.store.CreateFilePath(dest, false)
	if err != nil {
		return fmt.Errorf("cannot create destination file path: %v", err)
	}
	return s3.store.Upload(source, destination)
}

func (s3 *S3) Download(src, dest filestore.Filepath) error {
	return s3.store.Download(src, dest)
}

func (s3 *S3) LatestBackupName(dir string) (filestore.Filepath, error) {
	dirPath, err := s3.store.CreateFilePath(dir, true)
	if err != nil {
		return nil, fmt.Errorf("cannot create dir path: %v", err)
	}
	return s3.store.NewestFileOfType(dirPath, filestore.DB)
}

type Local struct {
	Path  string
	store provider.FileStore
}

func (fs *Local) Init() error {
	filestoreConfig := pc.LocalFileStoreConfig{
		DirPath: fs.Path,
	}
	config, err := filestoreConfig.Serialize()
	if err != nil {
		return fmt.Errorf("cannot serialize the LocalFileStoreConfig: %v", err)
	}

	filestore, err := provider.NewLocalFileStore(config)
	if err != nil {
		return fmt.Errorf("cannot create Local Filestore: %v", err)
	}
	fs.store = filestore
	return nil
}

func (fs *Local) Upload(src, dest string) error {
	source := &filestore.LocalFilepath{}
	if err := source.SetKey(src); err != nil {
		return fmt.Errorf("cannot set source key: %v", err)
	}
	destination, err := fs.store.CreateFilePath(dest, false)
	if err != nil {
		return fmt.Errorf("cannot create destination file path: %v", err)
	}
	return fs.store.Upload(source, destination)
}

func (fs *Local) Download(src, dest filestore.Filepath) error {
	return fs.store.Download(src, dest)
}

func (fs *Local) LatestBackupName(dir string) (filestore.Filepath, error) {
	dirPath, err := fs.store.CreateFilePath(dir, true)
	if err != nil {
		return nil, fmt.Errorf("cannot create dir path: %v", err)
	}
	return fs.store.NewestFileOfType(dirPath, filestore.DB)
}

type GCS struct {
	BucketName             string
	BucketPath             string
	CredentialFileLocation string
	Credentials            []byte
	store                  provider.FileStore
}

func (g *GCS) getDefaultCredentials() ([]byte, error) {
	if creds, err := gcp.DefaultCredentials(context.Background()); err != nil {
		return nil, err
	} else {
		return creds.JSON, nil
	}
}

func (g *GCS) checkEmptyCredentials() (map[string]interface{}, error) {
	var serializedCreds []byte
	creds := make(map[string]interface{})

	if bytes.Equal(g.Credentials, []byte("")) {
		var err error
		serializedCreds, err = g.getDefaultCredentials()
		if err != nil {
			return nil, fmt.Errorf("could not get default credentials: %v", err)
		}
	} else {
		serializedCreds = g.Credentials
	}

	err := json.Unmarshal(serializedCreds, &creds)
	if err != nil {
		return nil, fmt.Errorf("could not deserialize credentials: %v", err)
	}
	return creds, nil
}

func (g *GCS) Init() error {
	credentials, err := g.checkEmptyCredentials()
	if err != nil {
		return fmt.Errorf("failed to check credentials: %v", err)
	}

	filestoreConfig := pc.GCSFileStoreConfig{
		BucketName: g.BucketName,
		BucketPath: g.BucketPath,
		Credentials: pc.GCPCredentials{
			JSON: credentials,
		},
	}
	config, err := filestoreConfig.Serialize()
	if err != nil {
		return fmt.Errorf("cannot serialize GCS config: %v", err)
	}

	filestore, err := provider.NewGCSFileStore(config)
	if err != nil {
		return fmt.Errorf("cannot create GCS Filestore: %v", err)
	}
	g.store = filestore
	return nil
}

func (g *GCS) Upload(src, dest string) error {
	source := &filestore.LocalFilepath{}
	if err := source.SetKey(src); err != nil {
		return fmt.Errorf("cannot set source key: %v", err)
	}
	destination, err := g.store.CreateFilePath(dest, false)
	if err != nil {
		return fmt.Errorf("cannot create destination file path: %v", err)
	}
	return g.store.Upload(source, destination)
}

func (g *GCS) Download(src, dest filestore.Filepath) error {
	return g.store.Download(src, dest)
}

func (g *GCS) LatestBackupName(dir string) (filestore.Filepath, error) {
	dirPath, err := g.store.CreateFilePath(dir, true)
	if err != nil {
		return nil, fmt.Errorf("cannot create dir path: %v", err)
	}
	return g.store.NewestFileOfType(dirPath, filestore.DB)
}
