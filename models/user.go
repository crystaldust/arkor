package models

// Attention:
// Arkor did not support User Manamgement & Access Control yet

import (
	"github.com/jinzhu/gorm"
)

type Owner struct {
	gorm.Model `json:"-"`
	BucketName string `json:"-"`
	ObjectID   uint32 `json:"-"`
	// ContentKey  string `json:"-"`
	OwnerID     string `json:"id,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
}
