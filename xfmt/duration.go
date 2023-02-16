package xfmt

import (
	"fmt"
	"time"
)

var MillisecondAccuracy = 3

const years = time.Duration(365 * 24 * time.Hour)
const months = time.Duration(30 * 24 * time.Hour)
const weeks = time.Duration(7 * 24 * time.Hour)
const days = time.Duration(24 * time.Hour)
const hours = time.Hour
const mins = time.Minute
const secs = time.Second
const msecs = time.Millisecond

var units = map[time.Duration]string{
	years:  "y",
	months: "m",
	weeks:  "wk",
	days:   "d",
	hours:  "h",
	mins:   "m",
	secs:   "s",
	msecs:  "ms",
}
var pk = []time.Duration{years, months, weeks, days, hours, mins}

func Duration(d time.Duration) string {
	s := ""
	o := false
	r := d
	for _, p := range pk {
		a := r.Nanoseconds() / int64(p)
		o = o || a > 0
		if o && ((a > 0) || (p < days)) {
			s += fmt.Sprintf("%d%s ", a, units[p])
		}
		r -= time.Duration(a) * p
	}
	rs := r.Nanoseconds() / int64(secs)
	r -= time.Duration(rs) * secs

	rms := r.Nanoseconds() / int64(msecs)
	sfmt := fmt.Sprintf("%%d.%%0%dds", MillisecondAccuracy)
	s += fmt.Sprintf(sfmt, rs, rms)

	return s
}
