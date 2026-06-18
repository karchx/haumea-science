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

S3_PREFIX="raw"

def get_s3_client():
   return boto3.client(
        's3', 
        endpoint_url=S3_ENDPOINT, 
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

def extract_aries_data():
    temp_votable = "aries_temp.vot.gz"
    temp_parquet = "aries_star_cluster.parquet"
    s3_key = f"{S3_PREFIX}/{temp_parquet}"
    logging.warning(f"S3 path {s3_key}")
    try:
        coord = SkyCoord('02h07m10.40s', '+23d27m44.7s', frame='icrs')
        table_simbad = Simbad.query_region(coord, radius="1d")

        if table_simbad is None:
            raise ValueError("SIMBAD not data")

        valid_mask = ~table_simbad['ra'].mask & ~table_simbad['dec'].mask
        table_simbad = table_simbad[valid_mask]
        coords_simbad = SkyCoord(table_simbad['ra'], table_simbad['dec'], unit=(u.hourangle, u.deg))

        ra_center_deg = coord.ra.deg
        dec_center_deg = coord.dec.deg

        logging.info(f"ra_center: {ra_center_deg} dec_center_deg: {dec_center_deg}")
        query_gaia = f"""
        SELECT
           source_id, ra, dec, parallax, pmra, pmdec, phot_g_mean_mag
        FROM gaiadr3.gaia_source
        WHERE 1=CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', {ra_center_deg}, {dec_center_deg}, 1.0))
        """
        logging.info("Async and download in disk...")

        max_retries = 3

        for attempt in range(max_retries):
            try:
                job = Gaia.launch_job(query=query_gaia)
                table_gaia = job.get_results()
                table_gaia.write(temp_votable, format="votable", overwrite=True)
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error("Exceded retries")
                    raise e

                logging.error(f"retry {attempt}...")
                logging.error("Error", exc_info=True)


        logging.info("Process VOTable...")

        table_gaia = Table.read(temp_votable)
        if len(table_gaia) == 0:
            logging.warning("Empty VOTable")
            return

        coords_gaia = SkyCoord(table_gaia['ra'], table_gaia['dec'], unit=(u.deg, u.deg))

        # Cross match
        logging.info("Cross-match init...")

        max_sep = 0.25 * u.deg
        idx_simbad, idx_gaia, d2d, d3d = search_around_sky(coords_simbad, coords_gaia, max_sep)
        logging.info(f"Cross-match. {len(idx_simbad)} find clusters.")

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
            "pmra": np.array(matched_gaia['pmra']),
            "pmdec": np.array(matched_gaia['pmdec']),
            "phot_g_mean_mag": np.array(matched_gaia['phot_g_mean_mag'])
        })

        df_all = pl.concat([df_gaia, df_simbad], how="horizontal")

        df_all.write_parquet(temp_parquet, compression='snappy')

        logging.info(f"upload to minio")
        s3_client = get_s3_client()
        s3_client.upload_file(temp_parquet, BUCKET_NAME, s3_key)
        logging.info("EXTRACTION COMPLETED")
    except Exception as e:
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

