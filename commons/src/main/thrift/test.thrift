namespace java io.sanfran.wikiTrends.commons

struct WikiTrafficThrift {
	1: required string projectName,
	2: required string pageTitle,
	3: required i64 requestNumber,
	4: required i64 contentSize,
	5: required i16 year,
	6: required byte month,
	7: required byte day,
	8: required byte hour
}


