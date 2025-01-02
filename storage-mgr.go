package storagemgr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"time"

	"cloud.google.com/go/storage"

	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/google/uuid"
	"github.com/tadhunt/fsdb"
	"github.com/tadhunt/logger"
)

const (
	UPLOAD_STATE_INIT    = "init"
	UPLOAD_STATE_SUCCESS = "success"
	UPLOAD_STATE_DELETED = "deleted"

	UPLOAD_MAX_SIZE = 4 * 1024 * 1024 * 1024 // (2 GiB) Maximum size of an upload
)

var validUploadStatesFromClient = map[string]bool{
	//	UPLOAD_STATE_INIT: true,			// Client is not allowed to set this state (this state is set by newUploadURLHAndler)
	UPLOAD_STATE_SUCCESS: true,
	//	UPLOAD_STATE_DELETED: true,			// Client is not allowed to set this state (this state is set by a DELETE method in uploadHandler)
}

type StorageManager struct {
	log           logger.CompatLogWriter
	db            *fsdb.DBConnection
	downloadEmail string
	downloadKey   *Key
	uploadBucket  string
	uploadEmail   string
	uploadKey     *Key
}
type Key struct {
	Type                    string `json:"type"`
	ProjectID               string `json:"project_id"`
	PrivateKeyId            string `json:"private_key_id"`
	PrivateKey              string `json:"private_key"`
	ClientEmail             string `json:"client_email"`
	ClientID                string `json:"client_id"`
	AuthURI                 string `json:"auth_uri"`
	TokenURI                string `json:"token_uri"`
	AuthProviderX509CertURL string `json:"auth_provider_x509_cert_url"`
	ClientX509CertURL       string `json:"client_x509_cert_url"`
	UniverseDomain          string `json:"universe_domain"`

	raw                     []byte
}

type ObjectInfo struct {
	ID      string
	Name    string
	Size    string
	Created time.Time
}

type ListUploadsResponse struct {
	Objects []*ObjectInfo
}

type SignedURLResponse struct {
	SignedURL string
	UploadID  string
}

type DeleteUploadResponse struct {
}

type NewUploadURLRequest struct {
	Filename     string
	LastModified string
	Size         int
}

type UploadStateRequest struct {
	State string
}

type File struct {
	ID         string
	CreatorID  string
	SignedURL  string
	State      string
	LastUpdate time.Time
}

func NewStorageManager(log logger.CompatLogWriter, db *fsdb.DBConnection, downloadEmail string, rawDownloadKey []byte, uploadBucket string, uploadEmail string, rawUploadKey []byte) (*StorageManager, error) {
	downloadKey, err := decodeKey(rawDownloadKey)
	if err != nil {
		return nil, err
	}

	uploadKey, err := decodeKey(rawUploadKey)
	if err != nil {
		return nil, err
	}
	
	sm := &StorageManager{
		log:           log,
		db:            db,
		downloadEmail: downloadEmail,
		downloadKey:   downloadKey,
		uploadBucket:  uploadBucket,
		uploadEmail:   uploadEmail,
		uploadKey:     uploadKey,
	}

	return sm, nil
}

func decodeKey(src []byte) (*Key, error) {
	key := &Key{}
	err := json.Unmarshal(src, key)
	if err != nil {
		return nil, err
	}

	key.raw = src

	return key, nil
}

func (key *Key) Raw() []byte {
	return key.raw
}

func (sm *StorageManager) newDownloadSignedURL(bname string, objectID string) (string, error) {
	options := &storage.SignedURLOptions{
		GoogleAccessID: sm.downloadEmail,
		PrivateKey:     []byte(sm.downloadKey.PrivateKey),
		Method:         "GET",
		Expires:        time.Now().Add(5 * time.Minute),
	}

	url, err := storage.SignedURL(bname, objectID, options)
	if err != nil {
		return "", err
	}

	return url, nil
}

func (sm *StorageManager) NewDownloadURL(bname string, objectID string) (*SignedURLResponse, error) {
	sm.log.Infof("bucket %s object %s", bname, objectID)

	url, err := sm.newDownloadSignedURL(bname, objectID)
	if err != nil {
		return nil, err
	}

	response := &SignedURLResponse{
		SignedURL: url,
	}

	return response, nil
}

func (sm *StorageManager) GetObjectInfo(ctx context.Context, objectID string) (*ObjectInfo, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	bucket := client.Bucket(sm.uploadBucket)
	object := bucket.Object(objectID)

	attrs, err := object.Attrs(ctx)
	if err != nil {
		return nil, err
	}

	name, found := attrs.Metadata["file-name"]
	if !found || name == "" {
		name = path.Base(attrs.Name)
	}

	oinfo := &ObjectInfo{
		ID:      path.Base(attrs.Name),
		Name:    name,
		Size:    fmt.Sprintf("%d", attrs.Size),
		Created: attrs.Created,
	}

	return oinfo, nil
}

func (sm *StorageManager) ListUploads(ctx context.Context, requestUID string) (*ListUploadsResponse, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	bucket := client.Bucket(sm.uploadBucket)

	oprefix := fmt.Sprintf("%s/", requestUID)

	query := &storage.Query{
		Prefix: oprefix,
	}

	sm.log.Infof("bucket %#v query %#v", bucket, query)

	response := &ListUploadsResponse{
		Objects: []*ObjectInfo{},
	}

	it := bucket.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		name, found := attrs.Metadata["file-name"]
		if !found || name == "" {
			name = path.Base(attrs.Name)
		}

		oinfo := &ObjectInfo{
			ID:      path.Base(attrs.Name),
			Name:    name,
			Size:    fmt.Sprintf("%d", attrs.Size),
			Created: attrs.Created,
		}

		response.Objects = append(response.Objects, oinfo)
	}

	return response, nil
}

func (sm *StorageManager) NewUploadURL(ctx context.Context, requestUID string, request *NewUploadURLRequest) (*SignedURLResponse, error) {
	objectID, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	if request.Size < 1 || request.Size > UPLOAD_MAX_SIZE { // Limit size of uploaded files
		return nil, fmt.Errorf("bad size, expected 1..%d", UPLOAD_MAX_SIZE)
	}

	sm.log.Infof("bucket %s object %s", sm.uploadBucket, objectID)
	sm.log.Infof("uploadEmail %s uploadKey %s", sm.uploadEmail, sm.uploadKey)

	now := time.Now()
	options := &storage.SignedURLOptions{
		Scheme:         storage.SigningSchemeV4,
		Method:         "PUT",
		GoogleAccessID: sm.uploadEmail,
		PrivateKey:     []byte(sm.uploadKey.PrivateKey),
		Expires:        now.Add(10 * time.Minute),
		Headers: []string{
			"Content-Length: " + strconv.FormatInt(int64(request.Size), 10),
			"X-Goog-Meta-File-Name: " + request.Filename,
			"X-Goog-Meta-Last-Modified: " + request.LastModified,
			"X-Goog-Meta-Uploader: " + requestUID,
		},
	}

	url, err := storage.SignedURL(sm.uploadBucket, objectID.String(), options)
	if err != nil {
		return nil, err
	}

	response := &SignedURLResponse{
		SignedURL: url,
		UploadID:  objectID.String(),
	}

	upload := &File{
		ID:         objectID.String(),
		CreatorID:  requestUID,
		SignedURL:  url,
		State:      UPLOAD_STATE_INIT,
		LastUpdate: now,
	}

	dbpath := fmt.Sprintf("uploads/%s", objectID)
	err = sm.db.Add(ctx, dbpath, upload)
	if err != nil {
		return nil, err
	}

	sm.log.Infof("Upload State: %v", upload)

	return response, nil
}

func (sm *StorageManager) GetUpload(objectID string) (*SignedURLResponse, error) {
	sm.log.Infof("bucket %s object %s", sm.uploadBucket, objectID)

	u, err := sm.newDownloadSignedURL(sm.uploadBucket, objectID)
	if err != nil {
		return nil, err
	}

	response := &SignedURLResponse{
		SignedURL: u,
		UploadID:  objectID,
	}

	return response, nil
}

func (sm *StorageManager) DeleteUpload(ctx context.Context, objectID string) error {
	sm.log.Infof("bucket %s object %s", sm.uploadBucket, objectID)

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(sm.uploadBucket)

	err = bucket.Object(objectID).Delete(ctx)
	if err != nil {
		return err
	}

	dbpath := fmt.Sprintf("uploads/%s", objectID)
	err = sm.db.Delete(ctx, dbpath)
	if err != nil {
		if !fsdb.ErrorIsNotFound(err) {
			return err
		}
	}

	return nil
}

func (sm *StorageManager) SetUploadState(ctx context.Context, uploadID string, newState string) error {
	isValidState := validUploadStatesFromClient[newState]
	if !isValidState {
		return sm.log.ErrFmt("unsupported state: '%s'", newState)
	}

	dbpath := fmt.Sprintf("uploads/%s", uploadID)
	upload := &File{}
	err := sm.db.AtomicUpdate(ctx, dbpath, upload, func(ctx context.Context, dval interface{}) error {
		u, ok := dval.(*File)
		if !ok {
			return sm.log.ErrFmt("bad dval type, expected *Upload")
		}

		u.State = newState
		u.LastUpdate = time.Now()

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (sm *StorageManager) Read(ctx context.Context, objectID string, w http.ResponseWriter) error {
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON(sm.downloadKey.Raw()))
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(sm.uploadBucket)
	object := bucket.Object(objectID)

	reader, err := object.NewReader(ctx)
	if err != nil {
		return sm.log.ErrFmt("create reader %s/%s: %v", sm.uploadBucket, objectID, err)
	}
	defer reader.Close()

	_, err = io.Copy(w, reader)
	if err != nil {
		return sm.log.ErrFmt("stream object %s/%s: %v", sm.uploadBucket, objectID, err)
	}

	return nil
}
