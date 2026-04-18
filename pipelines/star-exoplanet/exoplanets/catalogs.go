package exoplanets

type Catalog struct {
	Name    string
	Columns []string
	OrderBy string
}

var Catalogs = []Catalog{
	{
		Name: "ps",
		Columns: []string{
			"pl_name",
			"pl_masse",
			"hostname",
			"ra",
			"dec",
			"gaia_dr3_id",
			"sy_snum",
			"sy_mnum",
			"sy_pnum",
			"soltype",
			"discoverymethod",
			"disc_year",
			"disc_locale",
			"disc_telescope",
			"obm_flag",
			"micro_flag",
			"releasedate",
			"elon",
			"elat",
		},
		OrderBy: "gaia_dr3_id",
	},
}
