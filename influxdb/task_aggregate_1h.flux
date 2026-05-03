option task = {name: "aggregate_1h", every: 1h, offset: 5m}

data = from(bucket: "velib")
    |> range(start: -task.every)
    |> filter(fn: (r) => r["_measurement"] == "station_status")
    |> aggregateWindow(every: 1m, fn: last, createEmpty: true)
    |> fill(usePrevious: true)

mean_data = data
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> map(fn: (r) => ({r with _field: r._field + "_mean"}))

min_data = data
    |> aggregateWindow(every: task.every, fn: min, createEmpty: false)
    |> map(fn: (r) => ({r with _field: r._field + "_min"}))

max_data = data
    |> aggregateWindow(every: task.every, fn: max, createEmpty: false)
    |> map(fn: (r) => ({r with _field: r._field + "_max"}))

union(tables: [mean_data, min_data, max_data])
    |> set(key: "_measurement", value: "station_status_1h")
    |> to(bucket: "velib_1h", org: "velib")
