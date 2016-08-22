package inner

import (
	"encoding/json"
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
		req.Fragments[i].ObjectMetaID = req.ObjectID
	}

	dbInstance := db.SQLDB.GetDB().(*gorm.DB)

	if req.BucketName != "" {
		var bucket models.Bucket
		result := dbInstance.Where("name = ?", req.BucketName).First(&bucket)
		if result.Error != nil {
			// TODO If error == "record not found", shall we create the bucket or report an error
			// log.Errorln(result.Error.Error())
			// return http.StatusInternalServerError, []byte("Internal Server Error")
		}
		if result.RowsAffected == 0 {
			dbInstance.Create(&models.Bucket{Name: req.BucketName, Objects: []models.ObjectMeta{req}})
		} else if err := dbInstance.Model(&bucket).Association("Objects").Append(&req).Error; err != nil {
			return http.StatusInternalServerError, []byte("Internal Server Error")
		}
	} else {
		var objectMeta models.ObjectMeta
		result := dbInstance.Where("object_id = ?", req.ObjectID).First(&objectMeta)
		var err error
		if result.RowsAffected == 0 {
			err = dbInstance.Create(&req).Error
		} else {
			err = dbInstance.Model(&objectMeta).Association("Fragments").Replace(req.Fragments).Error
		}
		if err != nil {
			log.Errorln(err.Error())
			return http.StatusInternalServerError, []byte("Internal Server Error")
		}
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
