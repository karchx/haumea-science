package exoplanets

type PlanetarySystem struct {
	PlName          string  `json:"pl_name"`
	PlMasse         float64 `json:"pl_masse"`
	Hostname        string  `json:"hostname"`
	Ra              float64 `json:"ra"`
	Dec             float64 `json:"dec"`
	GaiaDr3Id       string  `json:"gaia_dr3_id"`
	SySnum          int     `json:"sy_snum"`
	SyMnum          int     `json:"sy_mnum"`
	SyPnum          int     `json:"sy_pnum"`
	Soltype         string  `json:"soltype"`
	DiscoveryMethod string  `json:"discoverymethod"`
	DiscYear        int     `json:"disc_year"`
	DiscLocale      string  `json:"disc_locale"`
	DiscTelescope   string  `json:"disc_telescope"`
	ObmFlag         int     `json:"obm_flag"`
	MicroFlag       int     `json:"micro_flag"`
	ReleaseDate     string  `json:"releasedate"`
	Elon            float64 `json:"elon"`
	Elat            float64 `json:"elat"`
}
