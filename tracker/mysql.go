package tracker

import (
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// MySQLTracker represents a tracker that stores the import progress inside a MySQL database.
type MySQLTracker struct {
	GORMTracker
}

// Setup contains the storage preparations like connection etc. Is called only once at the very
// beginning of the work with the storage. As for the GORMTracker, it tests the connection / read
// access of the tracker, prepares everything like migrations.
func (t *GORMTracker) Setup() error {
	db, err := gorm.Open(mysql.Open(fmt.Sprintf("%s:%s@tcp(%s:%s)/?%s", t.Cfg.User, t.Cfg.Password, t.Cfg.Host, t.Cfg.Port, "parseTime=true")), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	if err != nil {
		return err
	}
	err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`  DEFAULT CHARACTER SET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_unicode_ci`;", t.Cfg.Database)).Error
	if err != nil {
		return err
	}
	mdb, err := db.DB()
	if err != nil {
		return err
	}
	mdb.Close()
	db, err = gorm.Open(mysql.Open(fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s", t.Cfg.User, t.Cfg.Password, t.Cfg.Host, t.Cfg.Port, t.Cfg.Database, "parseTime=true")), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	if err != nil {
		return err
	}
	db = db.Set("gorm:table_options", "CHARSET=utf8mb4 ENGINE=InnoDB COLLATE=utf8mb4_unicode_ci")
	t.client = db.Session(&gorm.Session{Logger: t.Cfg.Logger})
	if err := t.client.AutoMigrate(&iteration{}, &container{}, &operation{}, &issue{}); err != nil {
		return err
	}
	if t.Cfg.CleanupOnStart {
		return t.cleanup()
	}
	return nil
}
