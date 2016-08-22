package models

import (
	"time"

	"github.com/containerops/arkor/utils/db/mysql"
	"github.com/jinzhu/gorm"
)

type Bucket struct {
	gorm.Model   `json:"-"`
	Name         string       `json:"name,omitempty" gorm:"unique"`
	MaxKeys      string       `json:"max_keys,omitempty" gorm:"-"`
	KeyCount     string       `json:"key_count,omitempty" gorm:"-"`
	IsTruncated  bool         `json:"is_truncated,omitempty" gorm:"-"`
	CreationDate time.Time    `json:"creation_date,omitempty"`
	Objects      []ObjectMeta `json:"objects,omitempty" gorm:"ForeignKey:BucketName;AssociationForeignKey:Name"`
	Owner        Owner        `json:"-" gorm:"ForeignKey:BucketName;AssociationForeignKey:Name"`
}

type Content struct {
	BucketName   string    `json:"bucket_name"`
	Key          string    `json:"key"`
	LastModified time.Time `json:"last_modified"`
	ETag         string    `json:"eTag"`
	Type         string    `json:"type"`
	Size         int64     `json:"size"`
	StorageClass string    `json:"storage_class"`
}

type BucketListResponse struct {
	Type    string         `json:"type,omitempty"`
	Owner   Owner          `json:"owner,omitempty"`
	Buckets []BucketSimple `json:"buckets,omitempty"`
}

type BucketSimple struct {
	Name         string    `json:"name,omitempty"`
	CreationDate time.Time `json:"creation_date,omitempty"`
}

func (b *Bucket) Associate() {
	var bucket Bucket
	var object ObjectMeta
	var owner Owner
	mysqldb := mysql.MySQLInstance()
	mysqldb.Model(&bucket).Related(&object, "Objects")
	mysqldb.Model(&bucket).Related(&owner, "Owner")
	mysqldb.Model(&object).Related(&owner, "Owner")
}
