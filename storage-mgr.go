package storagemgr

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"

	"google.golang.org/api/iterator"

	"github.com/google/uuid"
	"github.com/tadhunt/logger"
	"github.com/tadhunt/fsdb"
)

const (
	UPLOAD_STATE_INIT    = "init"
	UPLOAD_STATE_SUCCESS = "success"
	UPLOAD_STATE_DELETED = "deleted"

	UPLOAD_MAX_SIZE = 4 * 1024 * 1024 * 1024 // (2 GiB) Maximum size of uploaded worlds
)

var validUploadStatesFromClient = map[string]bool{
	//	UPLOAD_STATE_INIT: true,			// Client is not allowed to set this state (this state is set by newUploadURLHAndler)
	UPLOAD_STATE_SUCCESS: true,
	//	UPLOAD_STATE_DELETED: true,			// Client is not allowed to set this state (this state is set by a DELETE method in uploadHandler)
}

type StorageManager struct {
	log     logger.CompatLogWriter
	db      *fsdb.DBConnection
	downloadEmail string
	downloadKey   []byte
	uploadBucket string
	uploadEmail  string
	uploadKey    []byte
}

type ObjectInfo struct {
	ID      string
	Name    string
	Size    string
	Created time.Time
}

type ListBackupsResponse struct {
	Objects []*ObjectInfo
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

type Upload struct {
	ID         string
	SignedURL  string
	State      string
	LastUpdate time.Time
}

func NewStorageManager(log logger.CompatLogWriter, db *fsdb.DBConnection, downloadEmail string, downloadKey []byte, uploadBucket string, uploadEmail string, uploadKey []byte) (*StorageManager, error) {
	sm := &StorageManager{
		log:     log,
		db:      db,
		downloadEmail: downloadEmail,
		downloadKey:   downloadKey,
		uploadBucket: uploadBucket,
		uploadEmail:  uploadEmail,
		uploadKey:    uploadKey,
	}

	return sm, nil
}

func (sm *StorageManager) newDownloadSignedURL(bname string, oname string) (string, error) {
	options := &storage.SignedURLOptions{
		GoogleAccessID: sm.downloadEmail,
		PrivateKey:     sm.downloadKey,
		Method:         "GET",
		Expires:        time.Now().Add(5 * time.Minute),
	}

	url, err := storage.SignedURL(bname, oname, options)
	if err != nil {
		return "", err
	}

	return url, nil
}

func (sm *StorageManager) NewDownloadURL(bname string, oname string) (*SignedURLResponse, error) {
	sm.log.Infof("bucket %s object %s", bname, oname)

	url, err := sm.newDownloadSignedURL(bname, oname)
	if err != nil {
		return nil, err
	}

	response := &SignedURLResponse{
		SignedURL: url,
	}

	return response, nil
}

func (sm *StorageManager) objectAttrsToObjectInfo(obj *storage.ObjectAttrs) *ObjectInfo {
	name, found := obj.Metadata["file-name"]
	if !found || name == "" {
		name = path.Base(obj.Name)
	}

	oinfo := &ObjectInfo{
		ID:      path.Base(obj.Name),
		Name:    name,
		Size:    fmt.Sprintf("%d", obj.Size),
		Created: obj.Created,
	}

	return oinfo
}

func (sm *StorageManager) GetNewestBackup(ctx context.Context, bname string, oprefix string) (*ObjectInfo, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(bname)

	query := &storage.Query{
		Prefix: oprefix,
	}

	sm.log.Infof("bucket %#v query %#v", bucket, query)

	var newestObj *storage.ObjectAttrs

	it := bucket.Objects(ctx, query)
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		if newestObj == nil || obj.Created.After(newestObj.Created) {
			newestObj = obj
		}
	}

	if newestObj == nil {
		return nil, sm.log.ErrFmt("no backups")
	}

	oinfo := sm.objectAttrsToObjectInfo(newestObj)

	return oinfo, nil
}

func (sm *StorageManager) ListBackups(ctx context.Context, bname string, oprefix string) (*ListBackupsResponse, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(bname)

	query := &storage.Query{
		Prefix: oprefix,
	}

	sm.log.Infof("bucket %#v query %#v", bucket, query)

	results := &ListBackupsResponse{
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

		backup := sm.objectAttrsToObjectInfo(attrs)

		results.Objects = append(results.Objects, backup)
	}

	return results, nil
}

func (sm *StorageManager) GetObjectInfo(ctx context.Context, rawurl string) (*ObjectInfo, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "gs" {
		return nil, sm.log.ErrFmt("got scheme '%s' expected 'gs'", u.Scheme)
	}

	bname := u.Host

	fields := strings.SplitAfterN(u.Path, "/", 2)
	if len(fields) != 2 {
		return nil, sm.log.ErrFmt("got %d fields expected 2 (%#v)", len(fields), fields)
	}
	oname := fields[1]

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(bname)
	object := bucket.Object(oname)

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
	uploadID, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	if request.Size < 1 || request.Size > UPLOAD_MAX_SIZE { // Limit size of uploaded files
		return nil, fmt.Errorf("bad size, expected 1..%d", UPLOAD_MAX_SIZE)
	}

	oname := fmt.Sprintf("%s/%s", requestUID, uploadID)

	sm.log.Infof("bucket %s object %s", sm.uploadBucket, oname)
	sm.log.Infof("uploadEmail %s uploadKey %s", sm.uploadEmail, sm.uploadKey)

	now := time.Now()
	options := &storage.SignedURLOptions{
		Scheme:         storage.SigningSchemeV4,
		Method:         "PUT",
		GoogleAccessID: sm.uploadEmail,
		PrivateKey:     sm.uploadKey,
		Expires:        now.Add(10 * time.Minute),
		Headers: []string{
			"Content-Length: " + strconv.FormatInt(int64(request.Size), 10),
			"X-Goog-Meta-File-Name: " + request.Filename,
			"X-Goog-Meta-Last-Modified: " + request.LastModified,
			"X-Goog-Meta-Uploader: " + requestUID,
		},
	}

	url, err := storage.SignedURL(sm.uploadBucket, oname, options)
	if err != nil {
		return nil, err
	}

	response := &SignedURLResponse{
		SignedURL: url,
		UploadID:  uploadID.String(),
	}

	upload := &Upload{
		ID:         uploadID.String(),
		SignedURL:  url,
		State:      UPLOAD_STATE_INIT,
		LastUpdate: now,
	}

	dbpath := fmt.Sprintf("/uploads/%s/%s", requestUID, uploadID)
	err = sm.db.Add(ctx, dbpath, upload)
	if err != nil {
		return nil, err
	}

	sm.log.Infof("Upload State: %v", upload)

	return response, nil
}

func (sm *StorageManager) GetUpload(requestUID string, uploadID string) (*SignedURLResponse, error) {
	oname := fmt.Sprintf("%s/%s", requestUID, uploadID)

	sm.log.Infof("bucket %s object %s", sm.uploadBucket, oname)

	u, err := sm.newDownloadSignedURL(sm.uploadBucket, oname)
	if err != nil {
		return nil, err
	}

	response := &SignedURLResponse{
		SignedURL: u,
		UploadID: uploadID,
	}

	return response, nil
}

func (sm *StorageManager) DeleteUpload(ctx context.Context, requestUID string, uploadID string) error {
	oname := fmt.Sprintf("%s/%s", requestUID, uploadID)

	sm.log.Infof("bucket %s object %s", sm.uploadBucket, oname)

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	bucket := client.Bucket(sm.uploadBucket)

	err = bucket.Object(oname).Delete(ctx)
	if err != nil {
		return err
	}

	dbpath := fmt.Sprintf("uploads/%s/%s", requestUID, uploadID)
	err = sm.db.Delete(ctx, dbpath)
	if err != nil {
		if !fsdb.ErrorIsNotFound(err) {
			return err
		}
	}

	return nil
}

func (sm *StorageManager) UploadStateHandler(ctx context.Context, requestUID string, uploadID string, state string) error {
	isValidState := validUploadStatesFromClient[state]
	if !isValidState {
		return sm.log.ErrFmt("unsupported state: '%s'", state)
	}

	dbpath := fmt.Sprintf("uploads/%s/%s", requestUID, uploadID)
	upload := &Upload{}
	err := sm.db.AtomicUpdate(ctx, dbpath, upload, func(ctx context.Context, dval interface{}) error {
		u, ok := dval.(*Upload)
		if !ok {
			return sm.log.ErrFmt("bad dval type, expected *Upload")
		}

		u.State = state
		u.LastUpdate = time.Now()

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}