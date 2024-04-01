package main

type stationDataInt64 struct {
	data map[string]*valuesInt64
}

func newStationDataInt64() *stationDataInt64 {
	return &stationDataInt64{data: make(map[string]*valuesInt64)}
}

func (sdf *stationDataInt64) addReading(name string, reading int64) {
	current, exists := sdf.data[name]
	if exists {
		newMin := min(current.min, reading)
		newMax := max(current.max, reading)

		current.min = newMin
		current.max = newMax
		current.sum += reading
		current.count += 1
	} else {
		sdf.data[name] = &valuesInt64{
			min:   reading,
			max:   reading,
			count: 1,
			sum:   reading,
		}
	}
}

func (sdf *stationDataInt64) StationNames() []string {
	return getMapKeys(sdf.data)
}

func (sdf *stationDataInt64) ValuesOf(name string) values {
	v, _ := sdf.data[name]
	return v
}

func (sdf *stationDataInt64) ConsumeLine(line []byte) {
	name, reading := parseLineInt64(line)
	sdf.addReading(name, reading)
}

func (sdf *stationDataInt64) Merge(other stationData) {
	switch o := other.(type) {
	case *stationDataInt64:
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
	default:
		panic("not supported")
	}
}

type valuesInt64 struct {
	min   int64
	max   int64
	count uint64
	sum   int64
}

func (v *valuesInt64) Min() float64 {
	return float64(v.min) / 10.0
}

func (v *valuesInt64) Mean() float64 {
	return (float64(v.sum) / 10.0) / float64(v.count)
}

func (v *valuesInt64) Max() float64 {
	return float64(v.max) / 10.0
}

func parseLineInt64(b []byte) (string, int64) {
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
