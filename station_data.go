package main

type stationData struct {
	data map[string]*values
}

func newStationData() *stationData {
	return &stationData{data: make(map[string]*values)}
}

func (sdf *stationData) addReading(name string, reading int64) {
	current, exists := sdf.data[name]
	if exists {
		newMin := min(current.min, reading)
		newMax := max(current.max, reading)

		current.min = newMin
		current.max = newMax
		current.sum += reading
		current.count += 1
	} else {
		sdf.data[name] = &values{
			min:   reading,
			max:   reading,
			count: 1,
			sum:   reading,
		}
	}
}

func (sdf *stationData) StationNames() []string {
	return getMapKeys(sdf.data)
}

func (sdf *stationData) ValuesOf(name string) *values {
	v, _ := sdf.data[name]
	return v
}

func (sdf *stationData) ConsumeLine(line []byte) {
	name, reading := parseLine(line)
	sdf.addReading(name, reading)
}

func (sdf *stationData) Merge(o *stationData) {
	for name, vals := range o.data {
		existing, exists := sdf.data[name]
		if exists {
			existing.max = max(existing.max, vals.max)
			existing.min = min(existing.min, vals.min)
			existing.sum += vals.sum
			existing.count += vals.count
		} else {
			sdf.data[name] = vals
		}
	}
}

type values struct {
	min   int64
	max   int64
	count uint64
	sum   int64
}

func (v *values) Min() float64 {
	return float64(v.min) / 10.0
}

func (v *values) Mean() float64 {
	return (float64(v.sum) / 10.0) / float64(v.count)
}

func (v *values) Max() float64 {
	return float64(v.max) / 10.0
}

func parseLine(b []byte) (string, int64) {
	pos := -1
	var reading int64 = 0
	negative := false
	capturingReading := false
	for i := 0; i < len(b); i++ {
		v := b[i]
		if v == ';' {
			capturingReading = true
			pos = i
			continue
		}

		if capturingReading {
			if b[i] == '.' {
				continue
			} else if b[i] == '-' {
				negative = true
				continue
			}
			value := int64(b[i] - 48)
			reading = (reading * 10.0) + value
		}
	}

	if negative {
		reading = reading * -1
	}

	name := string(b[0:pos])

	return name, reading
}
