package main

type stationDataFloat64 struct {
	data map[string]*valuesFloat64
}

func newStationDataFloat64() *stationDataFloat64 {
	return &stationDataFloat64{data: make(map[string]*valuesFloat64)}
}

func (sdf *stationDataFloat64) addReading(name string, reading float64) {
	current, exists := sdf.data[name]
	if exists {
		newMin := min(current.min, reading)
		newMax := max(current.max, reading)

		current.min = newMin
		current.max = newMax
		current.sum += reading
		current.count += 1
	} else {
		sdf.data[name] = &valuesFloat64{
			min:   reading,
			max:   reading,
			count: 1,
			sum:   reading,
		}
	}
}

func (sdf *stationDataFloat64) ConsumeLine(line []byte) {
	name, reading := parseLineFloat64(line)
	sdf.addReading(name, reading)
}

func (sdf *stationDataFloat64) StationNames() []string {
	return getMapKeys(sdf.data)
}

func (sdf *stationDataFloat64) ValuesOf(name string) values {
	v, _ := sdf.data[name]
	return v
}

type valuesFloat64 struct {
	min   float64
	max   float64
	count uint64
	sum   float64
}

func (v *valuesFloat64) Min() float64 {
	return v.min
}

func (v *valuesFloat64) Mean() float64 {
	return v.sum / float64(v.count)
}

func (v *valuesFloat64) Max() float64 {
	return v.max
}

func parseLineFloat64(b []byte) (string, float64) {
	pos := -1
	var reading float64 = 0.0
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
			value := float64(b[i] - 48)
			reading = (reading * 10.0) + value
		}
	}

	if negative {
		reading = reading * -1.0
	}

	reading = reading / 10.0

	name := string(b[0:pos])

	return name, reading

	//line := string(b)
	//pos := strings.Index(line, ";")
	//name := line[0:pos]
	//reading, _ := strconv.ParseFloat(line[pos+1:], 64)
	//return name, reading
}
