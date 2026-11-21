// Package activityday defines the calendar for daily activity snapshots.
package activityday

import (
	"time"
	_ "time/tzdata"
)

const Zone = "Asia/Shanghai"
const MaxWindowDays = 366

var beijing = loadLocation()

func loadLocation() *time.Location {
	location, err := time.LoadLocation(Zone)
	if err != nil {
		panic(err)
	}
	return location
}

// Date returns the Beijing date label used by activity IDs and database DATE columns.
func Date(at time.Time) string {
	return at.In(beijing).Format(time.DateOnly)
}

// Window returns the start of the first included day and the next midnight in Beijing.
func Window(now time.Time, days int) (from, to time.Time) {
	year, month, day := now.In(beijing).Date()
	today := time.Date(year, month, day, 0, 0, 0, 0, beijing)
	return today.AddDate(0, 0, 1-days), today.AddDate(0, 0, 1)
}
