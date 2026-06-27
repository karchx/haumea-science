import os
import logging
import sys
import boto3
from astroquery.gaia import Gaia
from astroquery.simbad import Simbad
from astropy.coordinates import SkyCoord, search_around_sky
from astropy.table import Table
import astropy.units as u
import polars as pl
import numpy as np

# Basic configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', 'http://minio.platform:9000')
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'gaia-source')

Gaia.ROW_LIMIT = -1

S3_PREFIX="raw"

def get_s3_client():
   return boto3.client(
        's3', 
        endpoint_url=S3_ENDPOINT, 
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

def calculate_r_final(ra_center, dec_center) -> int:
    TARGET_ROWS = 250000 # batch?
    R_MIN = 0.1 # Min radius (dense zones)
    R_MAX = 2.0 # Max radius (empty zones)
    R_TEST = 0.1 # static radius

    query_count = f"""
        SELECT count(1) AS n_obs
        FROM gaiadr3.gaia_source
        WHERE 1=CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', {ra_center}, {dec_center}, {R_TEST}))
    """
    job_count = Gaia.launch_job(query_count)
    res_count = job_count.get_results() or dict({"n_obs": [0]})
    n_test = res_count['n_obs'][0]
    
    if n_test == 0:
        raise ValueError("Invalid...")
    
    area_test = np.pi * (R_TEST**2)
    rho_gaia = n_test / area_test
    
    area_target = TARGET_ROWS / rho_gaia
    r_dynamic = np.sqrt(area_target/np.pi)

    return np.clip(r_dynamic, R_MIN, R_MAX)

def astroquery_data_missions(max_retries: int, temp_votable: str):
    def query_gaia(ra_center, dec_center, r_final):
        return f"""
            SELECT
                g.source_id, 
                g.ra, 
                g.dec, 
                g.parallax, 
                g.parallax_error,
                g.pmra, 
                g.pmdec,
                g.ruwe,
                g.phot_g_mean_mag,
                g.phot_bp_mean_mag, 
                g.phot_rp_mean_mag,
                tmass.j_m, 
                tmass.h_m, 
                tmass.ks_m,
                xmatch.angular_distance AS angular_distance_arcsec
            FROM gaiadr3.gaia_source AS g
            JOIN gaiadr3.tmass_psc_xsc_best_neighbour AS xmatch
              ON g.source_id = xmatch.source_id
            JOIN gaiadr3.tmass_psc_xsc_join AS xjoin 
                ON xmatch.clean_tmass_psc_xsc_oid = xjoin.clean_tmass_psc_xsc_oid
            JOIN gaiadr1.tmass_original_valid AS tmass 
                ON xjoin.original_psc_source_id = tmass.designation
            WHERE 1=CONTAINS(POINT('ICRS', g.ra, g.dec), CIRCLE('ICRS', {ra_center}, {dec_center}, {r_final}))
        """

    try:
        coord = SkyCoord('02h07m10.40s', '+23d27m44.7s', frame='icrs')
        table_simbad = Simbad.query_region(coord, radius='1d')
        if table_simbad is None:
            raise ValueError("SIMBAD not data")

        ra_center = coord.ra.deg
        dec_center = coord.dec.deg

        r_final = calculate_r_final(ra_center, dec_center)

        valid_mask = ~table_simbad['ra'].mask & ~table_simbad['dec'].mask
        table_simbad = table_simbad[valid_mask]
        coords_simbad = SkyCoord(table_simbad['ra'], table_simbad['dec'], unit=(u.hourangle, u.deg))
        logging.info(f"Ra Center: {ra_center:.2f} obs/deg2 | Dec Center: {dec_center:.2f} | Final Radius: {r_final:.4f} deg")

        query_gaia_str = query_gaia(ra_center, dec_center, r_final)
        logging.info("Async and download in disk...")
        logging.info(f"{query_gaia_str}")

        for attempt in range(max_retries):
            try:
                job = Gaia.launch_job(query=query_gaia_str)
                table_gaia = job.get_results()
                table_gaia.write(temp_votable, format="votable", overwrite=True)
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error("Exceded retries")
                    raise e

                logging.error(f"retry {attempt}...")
                logging.error("Error", exc_info=True)
        return coords_simbad, table_simbad
    except Exception:
        logging.error(f"Error extract data astroquery", exc_info=True)
        sys.exit(1)

def extract_aries_data(): 
    temp_votable = "aries_temp.vot.gz"
    temp_parquet = "aries_star_cluster.parquet"
    s3_key = f"{S3_PREFIX}/{temp_parquet}"
    logging.warning(f"S3 path {s3_key}")
    try:
        max_retries = 3
        coords_simbad, table_simbad = astroquery_data_missions(max_retries, temp_votable) 

        logging.info("Process VOTable...")
        table_gaia = Table.read(temp_votable)
        if len(table_gaia) == 0:
            logging.warning("Empty VOTable")
            return

        coords_gaia = SkyCoord(table_gaia['ra'], table_gaia['dec'], unit=(u.deg, u.deg))

        # Cross match
        logging.info("Cross-match init...")

        max_sep = 1.0 * u.deg
        idx_simbad, idx_gaia, d2d, d3d = search_around_sky(coords_simbad, coords_gaia, max_sep)
        logging.info(f"Cross-match. {len(idx_simbad)} find clusters.")

        if len(idx_simbad) == 0:
            logging.warning("No matches found between SIMBAD and Gaia. Aborting extraction.")
            return

        matched_simbad = table_simbad[idx_simbad]
        matched_gaia = table_gaia[idx_gaia]

        df_simbad = pl.DataFrame({
            "source_cluster": np.array(matched_simbad["main_id"]).astype(str)
        })

        df_gaia = pl.DataFrame({
            "source_id": np.array(matched_gaia['source_id']),
            "ra": np.array(matched_gaia['ra']),
            "dec": np.array(matched_gaia['dec']),
            "parallax": np.array(matched_gaia['parallax']),
            "parallax_error": np.array(matched_gaia['parallax_error']),
            "pmra": np.array(matched_gaia['pmra']),
            "pmdec": np.array(matched_gaia['pmdec']),
            "phot_g_mean_mag": np.array(matched_gaia['phot_g_mean_mag']),
            "phot_bp_mean_mag": np.array(matched_gaia["phot_bp_mean_mag"]),
            "phot_rp_mean_mag": np.array(matched_gaia["phot_rp_mean_mag"]),
            "j_m": np.array(matched_gaia["j_m"]),
            "h_m": np.array(matched_gaia["h_m"]),
            "ks_m": np.array(matched_gaia["ks_m"]),
            "ruwe": np.array(matched_gaia['ruwe']),
            "angular_distance_arcsec": np.array(matched_gaia['angular_distance_arcsec'])
        })
        df_all = pl.concat([df_gaia, df_simbad], how="horizontal")

        df_all.write_parquet(temp_parquet, compression='snappy')

        logging.info(f"upload to minio")
        s3_client = get_s3_client()
        s3_client.upload_file(temp_parquet, BUCKET_NAME, s3_key)
        logging.info("EXTRACTION COMPLETED")
    except Exception:
        logging.error(f"Error in load", exc_info=True)
        sys.exit(1)
    finally:
        for f in [temp_votable, temp_parquet]:
            if os.path.exists(f):
                os.remove(f)

def main():
    logging.info("="*50)
    extract_aries_data()
    logging.info("="*50)

if __name__ == "__main__":
    main()

