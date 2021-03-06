SELECT
	date_format(dts.start_time_slot, "yyyy-MM-dd'T'hh:mm:ss") as TimeSlotStart,
	Location,
	TempMin,
	TempMax,
	TempAvg,
	TempCnt,
	Presence,
	PresenceCnt
FROM
	dates dts
	LEFT OUTER JOIN (
		SELECT
			window.start,
			location_id as Location,
			ifnull(format_number(min(case when channel_type == "temperature" then ((value - 32) * 5 / 9) else null end), 2), "") as TempMin,
			ifnull(format_number(max(case when channel_type == "temperature" then ((value - 32) * 5 / 9) else null end), 2), "") as TempMax,
			ifnull(format_number(avg(case when channel_type == "temperature" then ((value - 32) * 5 / 9) else null end), 2), "") as TempAvg,
			count(case when channel_type == "temperature" then value else null end) as TempCnt,
			count(case when channel_type == "presence" then value else null end) > 0 as Presence,
			count(case when channel_type == "presence" then value else null end) as PresenceCnt
		FROM
			values v JOIN locations l
		ON
			v.sensor_id == l.location_sensor_id AND v.channel_id == l.location_channel_id
		GROUP BY
			window(create_time, '15 minutes', '15 minutes'), location_id
		ORDER BY
			window.start, location_id
	)  vl
	ON dts.start_time_slot = vl.start
ORDER BY
    dts.start_time_slot