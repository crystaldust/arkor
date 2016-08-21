package inner

import (
	"encoding/json"
	// "fmt"
	"net/http"
	// "time"

	"github.com/Sirupsen/logrus"
	"github.com/jinzhu/gorm"
	"gopkg.in/macaron.v1"

	"github.com/containerops/arkor/models"
	"github.com/containerops/arkor/utils"
	"github.com/containerops/arkor/utils/db"
)

func AllocateFileID(ctx *macaron.Context, log *logrus.Logger) (int, []byte) {
	m := make(map[string]string)
	m["file_id"] = utils.MD5ID()

	result, err := json.Marshal(m)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	return http.StatusOK, result
}

func PutObjectInfoHandler(ctx *macaron.Context, req models.ObjectMeta, log *logrus.Logger) (int, []byte) {
	if len(req.Fragments) == 0 {
		return http.StatusBadRequest, []byte("Invalid Parameters")
	}

	for i := range req.Fragments {
		req.Fragments[i].FragmentID = utils.MD5ID()
	}

	// Find the record in db
	dbInstance := db.SQLDB.GetDB().(*gorm.DB)
	var objectMeta models.ObjectMeta
	dbInstance.Where("id = ?", req.ID).First(&objectMeta)

	var err error
	if &objectMeta == nil || objectMeta.ID == "" { // Create new record
		err = db.SQLDB.Create(&req)
	} else { // Update the objectMeta
		err = dbInstance.Model(&objectMeta).Association("Fragments").Replace(req.Fragments).Error
	}

	if err != nil {
		log.Errorln(err.Error())
		return http.StatusInternalServerError, nil
	}

	return http.StatusOK, nil
}

func GetObjectInfoHandler(ctx *macaron.Context, log *logrus.Logger) (int, []byte) {
	objectID := ctx.Params(":object")

	var objectMeta models.ObjectMeta
	dbInstance := db.SQLDB.GetDB().(*gorm.DB)

	if err := dbInstance.Where("id = ?", objectID).Preload("Fragments").First(&objectMeta).Error; err != nil {
		log.Errorln(err.Error())
		if err.Error() == "record not found" {
			return http.StatusNotFound, []byte("Object NOT found")
		}
		return http.StatusInternalServerError, nil
	}

	result, err := json.Marshal(objectMeta)
	if err != nil {

		log.Errorln(err.Error())
		return http.StatusInternalServerError, nil
	}

	log.Println("The query result:", objectMeta)

	ctx.Resp.Header().Set("Content-Type", "application/json")
	return http.StatusOK, result
}
