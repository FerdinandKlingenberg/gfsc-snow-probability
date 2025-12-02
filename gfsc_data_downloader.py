#!/usr/bin/env python3
"""
Unified GFSC Data Downloader
============================

This script downloads Gap-Filled Fractional Snow Cover (GFSC) data from both:
1. WEkEO HDA API (for data before January 20, 2025)
2. S3 Copernicus HRWSI (for data from January 20, 2025 onwards)

The script automatically detects which source to use based on the date range
and organizes downloads to match gfsc_snow_probability_processor.py expectations.

Output Directory Structure:
- GFSC-2017-2024/  Contains product directories for years 2017-2024
- GFSC-2025/       Contains product directories for year 2025+

Usage:
1. Configure your credentials and parameters in the configuration section
2. Run: python gfsc_data_downloader.py
"""

import os
import re
import sys
import time
import zipfile
import shutil
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# WEkEO HDA imports
try:
    from hda import Client, Configuration
    WEKEO_AVAILABLE = True
except ImportError:
    WEKEO_AVAILABLE = False
    print("Warning: WEkEO HDA library not available. Install with: pip install hda")

# S3 imports
try:
    import boto3
    from botocore.exceptions import ClientError, EndpointConnectionError
    from retry import retry
    from tqdm import tqdm
    import geopandas as gpd
    from shapely import GEOSException
    from pyproj.crs import CRSError
    from pyogrio.errors import DataSourceError
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False
    print("Warning: S3 libraries not available. Install with: pip install boto3 retry tqdm geopandas shapely pyproj pyogrio")

# ============================================================================
# CONFIGURATION - MODIFY THIS SECTION FOR YOUR NEEDS
# ============================================================================

# WEkEO Credentials (for data before 2025-01-20)
# Opprett bruker på: https://identity.dataspace.copernicus.eu/
WEKEO_USER = "ditt_brukernavn"
WEKEO_PASSWORD = "ditt_passord"

# S3 Credentials (for data from 2025-01-20 onwards)
S3_ACCESS_KEY = 'c4ae60af7b144053803c618a8860f7c9' 
S3_SECRET_KEY = 'dcb3ba1f6eab45aaaec5802feef5e2e4'
S3_ENDPOINT_URL = "https://s3.WAW3-2.cloudferro.com"
S3_BUCKET = "HRWSI"

# Processing parameters
YEARS_TO_PROCESS = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
MONTHS_TO_PROCESS = [5]  # April = [4], April-June = [4, 5, 6]
TILES_TO_PROCESS = ['T32VML', 'T32VMM', 'T32VNL', 'T32VNM']

# Bounding box (prefered, compared to tiles)
BBOX = [
    8.8543,   # min longitude
    59.3512,  # min latitude
    9.4976,   # max longitude
    59.7796   # max latitude
]

# Filtering method
# NOTE: WEkEO requires bbox (tile filtering doesn't work), S3 can use either
USE_TILES = False   # Use MGRS tile filtering (works for S3 only)
USE_BBOX = True     # Use bounding box filtering (REQUIRED for WEkEO, works for S3)

# Output directory structure (compatible with gfsc_snow_probability_processor.py)
OUTPUT_BASE_DIR = "./gfsc_data"

# Download settings
TEST_MODE = False  # Set to True to download only first few days
TEST_DAYS = 3      # Number of days in test mode
DRY_RUN = False    # Set to True to test without downloading

# Transition date between WEkEO and S3
TRANSITION_DATE = datetime(2025, 1, 20)

# MGRS tiles file for S3 spatial filtering
MGRS_FILE = "MGRS_tiles.gpkg"

# WEkEO rate limiting (100 downloads per hour)
WEKEO_RATE_LIMIT = 100  # Max downloads per hour
WEKEO_COOLDOWN_MINUTES = 61  # Minutes to wait when rate limit hit (60 + 1 buffer)

# Output directory names (used for filtering and display)
OUTPUT_DIR_NAMES = ['GFSC-2017-2024', 'GFSC-2025']

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_output_directory(year: int) -> Path:
    """
    Get output directory based on year to match standalone_gfsc_processor.py structure
    - Years 2017-2024: GFSC-2017-2024/
    - Year 2025+: GFSC-2025/
    """
    base_path = Path(OUTPUT_BASE_DIR)

    if year >= 2025:
        output_dir = base_path / "GFSC-2025"
    else:
        output_dir = base_path / "GFSC-2017-2024"

    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def extract_and_organize_zips(download_dir: Path, target_dir: Path) -> int:
    """
    Extract zip files and move product directories to target location.
    Skips extraction if product directory already exists.
    Returns number of files extracted.
    """
    extracted_count = 0
    skipped_count = 0

    zip_files = list(download_dir.glob("*.zip"))

    if not zip_files:
        return 0

    print(f"  Found {len(zip_files)} zip files to extract")

    for zip_path in zip_files:
        try:
            # Expected product directory name (same as zip stem)
            expected_product_name = zip_path.stem
            target_product_dir = target_dir / expected_product_name

            # First check: Skip if product already exists using expected name (zip stem)
            if target_product_dir.exists():
                skipped_count += 1
                zip_path.unlink()  # Delete the zip since it's already extracted
                continue

            temp_extract_dir = download_dir / f"temp_extract_{zip_path.stem}"
            temp_extract_dir.mkdir(exist_ok=True)

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_extract_dir)

            product_dirs = [d for d in temp_extract_dir.iterdir() if d.is_dir()]

            if product_dirs:
                product_dir = product_dirs[0]
                final_target = target_dir / product_dir.name

                # Second check: The extracted dir name may differ from zip stem
                # (e.g., if zip contains a differently named directory)
                if final_target.exists():
                    skipped_count += 1
                else:
                    shutil.move(str(product_dir), str(final_target))
                    extracted_count += 1

            shutil.rmtree(temp_extract_dir)
            zip_path.unlink()

        except Exception as e:
            print(f"  Warning: Failed to extract {zip_path.name}: {e}")
            continue

    if skipped_count > 0:
        print(f"  Skipped {skipped_count} already-extracted products")

    return extracted_count


def split_date_range_by_source(start_date: datetime, end_date: datetime) -> Tuple[Optional[Tuple], Optional[Tuple]]:
    """
    Split date range into WEkEO and S3 portions based on transition date.
    Returns: (wekeo_range, s3_range) where each is (start, end) or None
    """
    wekeo_range = None
    s3_range = None

    if end_date < TRANSITION_DATE:
        # Entire range is WEkEO
        wekeo_range = (start_date, end_date)
    elif start_date >= TRANSITION_DATE:
        # Entire range is S3
        s3_range = (start_date, end_date)
    else:
        # Split range
        wekeo_range = (start_date, TRANSITION_DATE - timedelta(days=1))
        s3_range = (TRANSITION_DATE, end_date)

    return wekeo_range, s3_range

# ============================================================================
# WEKEO HDA DOWNLOADER
# ============================================================================

class WEkEODownloader:
    """Handles downloads from WEkEO HDA API (pre-2025 data)"""

    def __init__(self, user: str, password: str):
        if not WEKEO_AVAILABLE:
            raise ImportError("WEkEO HDA library not available")

        self.user = user
        self.password = password
        self.client = None

    def connect(self):
        """Initialize WEkEO HDA client"""
        print("Connecting to WEkEO HDA API...")
        conf = Configuration(user=self.user, password=self.password)
        self.client = Client(config=conf)
        print("✓ Connected to WEkEO HDA API")

    def build_query(self, year: int, month: int, start_date: str, end_date: str,
                   tiles: List[str] = None, bbox: List[float] = None) -> Dict:
        """Build WEkEO query - ALWAYS use bbox, tile filtering doesn't work well"""
        query = {
            "dataset_id": "EO:CRYO:DAT:HRSI:GFSC",
            "startdate": start_date,
            "enddate": end_date,
            "itemsPerPage": 200,
            "startIndex": 0
        }

        # WEkEO GFSC: tile filtering (productIdentifier/tileId) doesn't work properly
        # Always use bbox instead
        if bbox:
            query["bbox"] = bbox
        else:
            # If no bbox provided but tiles given, we should still use bbox
            # For now, just use the query without spatial filter (will return all tiles)
            print(f"  [WEkEO] Warning: No bbox provided, downloading all tiles in date range")

        return query

    def download(self, year: int, month: int, start_date: datetime, end_date: datetime,
                tiles: List[str] = None, bbox: List[float] = None, dry_run: bool = False) -> int:
        """Download data from WEkEO for specified period with automatic rate limit handling"""

        month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                      7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

        start_str = start_date.strftime("%Y-%m-%dT00:00:00.000Z")
        end_str = (end_date + timedelta(days=1) - timedelta(seconds=1)).strftime("%Y-%m-%dT23:59:59.999Z")

        print(f"  [WEkEO] Searching {month_names[month]} {year}: {start_date.date()} to {end_date.date()}")

        query = self.build_query(year, month, start_str, end_str, tiles, bbox)

        try:
            matches = self.client.search(query)
            num_matches = len(matches)

            print(f"  [WEkEO] Found: {num_matches} files")

            if num_matches == 0:
                return 0

            if dry_run:
                print(f"  [WEkEO] Dry run - would download {num_matches} files")
                return 0

            base_path = Path(OUTPUT_BASE_DIR)
            base_path.mkdir(exist_ok=True)
            year_output_dir = get_output_directory(year)

            total_extracted = 0
            attempt = 0
            max_attempts = 20  # Safety limit (20 hours max)

            while attempt < max_attempts:
                attempt += 1

                # Check which products are already extracted
                existing_products = set(d.name for d in year_output_dir.iterdir() if d.is_dir())

                # Count how many still need downloading
                num_already_extracted = 0
                for match in matches:
                    if hasattr(match, 'results') and len(match.results) > 0:
                        product_id = match.results[0].get('id', '')
                        if product_id in existing_products:
                            num_already_extracted += 1

                num_to_download = num_matches - num_already_extracted

                if num_to_download == 0:
                    print(f"  [WEkEO] ✓ All {num_matches} products extracted")
                    return total_extracted

                if attempt == 1:
                    print(f"  [WEkEO] {num_to_download} products need to be downloaded")
                    if num_to_download > WEKEO_RATE_LIMIT:
                        print(f"  [WEkEO] ⚠ Note: {num_to_download} files exceeds {WEKEO_RATE_LIMIT}/hour limit")
                        print(f"  [WEkEO] Script will wait automatically between batches")
                else:
                    print(f"  [WEkEO] Attempt {attempt}: {num_to_download} products remaining")

                print(f"  [WEkEO] Submitting download request...")
                print(f"  [WEkEO] (This may take several minutes)")

                try:
                    # Count zips before download
                    zips_before = set(base_path.glob("*.zip"))

                    # Download - WEkEO will download what it can within quota
                    matches.download(download_dir=str(base_path))

                    # Check for new zip files
                    zips_after = set(base_path.glob("*.zip"))
                    new_zips = zips_after - zips_before

                    # Handle files without extension (WEkEO quirk)
                    for item in base_path.iterdir():
                        if item.is_file() and not item.suffix and item.name not in OUTPUT_DIR_NAMES:
                            zip_path = item.with_suffix('.zip')
                            item.rename(zip_path)
                            new_zips.add(zip_path)

                    num_downloaded = len(new_zips)
                    print(f"  [WEkEO] ✓ Downloaded {num_downloaded} files")

                    # Extract downloaded files
                    if num_downloaded > 0:
                        print(f"  [WEkEO] Extracting to: {year_output_dir}")
                        extracted = extract_and_organize_zips(base_path, year_output_dir)
                        print(f"  [WEkEO] ✓ Extracted {extracted} products")
                        total_extracted += extracted

                    # Check if we're done or hit rate limit
                    # Re-check remaining after extraction
                    existing_products = set(d.name for d in year_output_dir.iterdir() if d.is_dir())
                    num_still_remaining = 0
                    for match in matches:
                        if hasattr(match, 'results') and len(match.results) > 0:
                            product_id = match.results[0].get('id', '')
                            if product_id not in existing_products:
                                num_still_remaining += 1

                    if num_still_remaining == 0:
                        print(f"  [WEkEO] ✓ All products downloaded and extracted")
                        return total_extracted

                    # If we downloaded 0 new files but still have remaining, we hit rate limit
                    if num_downloaded == 0 and num_still_remaining > 0:
                        print(f"  [WEkEO] ⏳ Rate limit hit - {num_still_remaining} products remaining")
                        print(f"  [WEkEO] Waiting {WEKEO_COOLDOWN_MINUTES} minutes for quota reset...")
                        print(f"  [WEkEO] (Started waiting at {datetime.now().strftime('%H:%M:%S')})")
                        
                        # Wait with progress indicator
                        for minute in range(WEKEO_COOLDOWN_MINUTES):
                            remaining = WEKEO_COOLDOWN_MINUTES - minute
                            print(f"  [WEkEO] ⏳ {remaining} minutes remaining...", end='\r')
                            time.sleep(60)
                        
                        print(f"  [WEkEO] ✓ Quota reset - resuming download at {datetime.now().strftime('%H:%M:%S')}")
                        continue  # Retry the download

                    # If we downloaded some files, continue without waiting
                    # (next iteration will check if more remain)

                except Exception as download_error:
                    print(f"  [WEkEO] ✗ Download error: {download_error}")
                    print(f"  [WEkEO] Waiting {WEKEO_COOLDOWN_MINUTES} minutes before retry...")
                    time.sleep(WEKEO_COOLDOWN_MINUTES * 60)
                    continue

            print(f"  [WEkEO] ⚠ Max attempts reached - some files may not be downloaded")
            return total_extracted

        except Exception as e:
            print(f"  [WEkEO] ✗ Error: {e}")
            return 0

# ============================================================================
# S3 DOWNLOADER
# ============================================================================

class S3Downloader:
    """Handles downloads from S3 Copernicus HRWSI (2025+ data)"""

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        if not S3_AVAILABLE:
            raise ImportError("S3 libraries not available")

        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.bucket = bucket
        self.client = None

    def connect(self):
        """Initialize S3 client"""
        print("Connecting to S3 Copernicus HRWSI...")
        self.client = boto3.resource(
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=self.endpoint_url
        )
        print("✓ Connected to S3 Copernicus HRWSI")

    def find_mgrs_tiles_from_bbox(self, bbox: List[float]) -> List[str]:
        """Convert bbox to MGRS tiles using MGRS_tiles.gpkg"""
        if not Path(MGRS_FILE).exists():
            print(f"  Warning: MGRS file not found: {MGRS_FILE}")
            print(f"  Falling back to configured tiles: {TILES_TO_PROCESS}")
            return TILES_TO_PROCESS

        try:
            tile_gpd = gpd.read_file(MGRS_FILE)

            # Create polygon from bbox (assuming EPSG:4326)
            from shapely.geometry import box
            bbox_geom = box(bbox[0], bbox[1], bbox[2], bbox[3])
            poly_gpd = gpd.GeoDataFrame(geometry=[bbox_geom], crs="EPSG:4326")
            poly_gpd = poly_gpd.to_crs(tile_gpd.crs)

            tile_gpd['foundTiles'] = tile_gpd.index
            intersecting = poly_gpd.sjoin(tile_gpd, how='inner')['foundTiles']
            found_tiles_gpd = tile_gpd[tile_gpd.foundTiles.isin(intersecting)]

            tiles = found_tiles_gpd.Name.to_list()
            print(f"  Found {len(tiles)} MGRS tiles from bbox: {tiles}")
            return tiles

        except Exception as e:
            print(f"  Warning: Failed to convert bbox to tiles: {e}")
            print(f"  Falling back to configured tiles: {TILES_TO_PROCESS}")
            return TILES_TO_PROCESS

    def download(self, year: int, month: int, start_date: datetime, end_date: datetime,
                tiles: List[str] = None, bbox: List[float] = None, dry_run: bool = False) -> int:
        """Download data from S3 for specified period"""

        month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                      7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

        # Determine tiles to download
        if bbox and not tiles:
            tiles = self.find_mgrs_tiles_from_bbox(bbox)

        if not tiles:
            print(f"  [S3] Error: No tiles specified")
            return 0

        print(f"  [S3] Searching {month_names[month]} {year}: {start_date.date()} to {end_date.date()}")
        print(f"  [S3] Tiles: {tiles}")

        total_downloaded = 0

        for tile in tiles:
            # Remove 'T' prefix if present
            tile_clean = tile[1:] if tile.startswith('T') else tile

            # Search for products in S3
            # Product type for GFSC is "GFSC"
            prefix = f"GFSC/{tile_clean}"

            marker_start = f"{prefix}/{start_date.year}/{start_date.strftime('%m')}/{start_date.strftime('%d')}"
            marker_end = f"{prefix}/{(end_date + timedelta(days=1)).year}/{(end_date + timedelta(days=1)).strftime('%m')}/{(end_date + timedelta(days=1)).strftime('%d')}"

            try:
                contents_to_filter = [obj for obj in self.client.Bucket(self.bucket).objects.filter(Prefix=prefix, Marker=marker_start)]
                contents_filter = [obj for obj in self.client.Bucket(self.bucket).objects.filter(Prefix=prefix, Marker=marker_end)]
                contents = [item for item in contents_to_filter if item not in contents_filter]

                # Group by product directory
                products = {}
                for content in contents:
                    product_dir = os.path.dirname(content.key)
                    if product_dir not in products:
                        products[product_dir] = []
                    products[product_dir].append(content)

                print(f"    [S3] Tile {tile}: Found {len(products)} products")

                if len(products) > 0 and not dry_run:
                    year_output_dir = get_output_directory(year)

                    for product_dir, files in tqdm(products.items(), desc=f"    [S3] {tile}"):
                        product_name = os.path.basename(product_dir)
                        local_product_dir = year_output_dir / product_name
                        local_product_dir.mkdir(exist_ok=True)

                        for obj in files:
                            file_name = os.path.basename(obj.key)
                            local_file = local_product_dir / file_name

                            try:
                                self.client.Bucket(self.bucket).download_file(obj.key, str(local_file))
                            except Exception as e:
                                print(f"      Warning: Failed to download {file_name}: {e}")

                    print(f"    [S3] ✓ Downloaded {len(products)} products for tile {tile}")
                    total_downloaded += len(products)

            except Exception as e:
                print(f"    [S3] ✗ Error downloading tile {tile}: {e}")

        return total_downloaded

# ============================================================================
# UNIFIED DOWNLOADER
# ============================================================================

class UnifiedGFSCDownloader:
    """Unified downloader that handles both WEkEO and S3 sources"""

    def __init__(self):
        self.wekeo = None
        self.s3 = None

        # Initialize WEkEO if credentials provided
        if WEKEO_AVAILABLE and WEKEO_USER and WEKEO_PASSWORD:
            try:
                self.wekeo = WEkEODownloader(WEKEO_USER, WEKEO_PASSWORD)
                self.wekeo.connect()
            except Exception as e:
                print(f"Warning: Failed to initialize WEkEO: {e}")

        # Initialize S3 if credentials provided
        if S3_AVAILABLE and S3_ACCESS_KEY and S3_SECRET_KEY:
            try:
                self.s3 = S3Downloader(S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT_URL, S3_BUCKET)
                self.s3.connect()
            except Exception as e:
                print(f"Warning: Failed to initialize S3: {e}")

    def download_month(self, year: int, month: int, tiles: List[str] = None,
                      bbox: List[float] = None, test_mode: bool = False,
                      test_days: int = 3, dry_run: bool = False) -> Dict:
        """Download data for a specific month, automatically choosing source(s)"""

        # Calculate date range
        start_date = datetime(year, month, 1)

        if test_mode:
            end_date = start_date + timedelta(days=test_days)
        else:
            if month == 12:
                end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(year, month + 1, 1) - timedelta(days=1)

        # Split by source
        wekeo_range, s3_range = split_date_range_by_source(start_date, end_date)

        result = {
            'year': year,
            'month': month,
            'wekeo_files': 0,
            's3_files': 0,
            'total_files': 0,
            'status': 'SUCCESS'
        }

        # Download from WEkEO if applicable
        if wekeo_range and self.wekeo:
            try:
                wekeo_files = self.wekeo.download(
                    year, month, wekeo_range[0], wekeo_range[1],
                    tiles=tiles, bbox=bbox, dry_run=dry_run
                )
                result['wekeo_files'] = wekeo_files
            except Exception as e:
                print(f"  [WEkEO] Error: {e}")
                result['status'] = f'WEkEO ERROR: {str(e)[:50]}'

        # Download from S3 if applicable
        if s3_range and self.s3:
            try:
                s3_files = self.s3.download(
                    year, month, s3_range[0], s3_range[1],
                    tiles=tiles, bbox=bbox, dry_run=dry_run
                )
                result['s3_files'] = s3_files
            except Exception as e:
                print(f"  [S3] Error: {e}")
                result['status'] = f'S3 ERROR: {str(e)[:50]}'

        result['total_files'] = result['wekeo_files'] + result['s3_files']

        return result

    def download_all(self, years: List[int], months: List[int], tiles: List[str] = None,
                    bbox: List[float] = None, test_mode: bool = False,
                    test_days: int = 3, dry_run: bool = False):
        """Download all specified data"""

        print("=" * 70)
        print("UNIFIED GFSC DATA DOWNLOAD")
        print("=" * 70)
        print(f"Years: {years}")
        print(f"Months: {months}")
        if test_mode:
            print(f"⚠ TEST MODE: Downloading only first {test_days} days of each month")

        if tiles:
            print(f"Tiles: {tiles}")
        if bbox:
            print(f"Bounding box: {bbox}")

        print(f"Output base directory: {OUTPUT_BASE_DIR}")
        print(f"Dry run: {dry_run}")
        print(f"Data transition date: {TRANSITION_DATE.date()} (WEkEO → S3)")
        print()

        download_summary = []
        total_files = 0
        start_time = time.time()

        for year in years:
            for month in months:
                month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                              7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
                month_name = month_names[month]

                print("=" * 70)
                print(f"Processing {month_name} {year}")
                print("=" * 70)

                result = self.download_month(
                    year, month, tiles=tiles, bbox=bbox,
                    test_mode=test_mode, test_days=test_days, dry_run=dry_run
                )

                download_summary.append(result)
                total_files += result['total_files']

        # Final summary
        total_time = time.time() - start_time

        print("\n" + "=" * 70)
        print("DOWNLOAD SUMMARY")
        print("=" * 70)
        print(f"Total files downloaded: {total_files}")
        print(f"Total time: {total_time / 60:.1f} minutes")
        print()

        print("Details:")
        for item in download_summary:
            month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                          7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
            month_name = month_names[item['month']]
            print(f"  {item['year']} {month_name:>3}: WEkEO={item['wekeo_files']:>3}, S3={item['s3_files']:>3}, Total={item['total_files']:>3} - {item['status']}")

        print()
        print(f"Output directory: {Path(OUTPUT_BASE_DIR).absolute()}")
        print(f"  - GFSC-2017-2024/: Years 2017-2024 (old format)")
        print(f"  - GFSC-2025/: Year 2025+ (new format)")

        if dry_run:
            print("\n⚠ DRY RUN MODE - No files were actually downloaded")
            print("Set DRY_RUN = False to download files")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""

    # Create base output directory
    Path(OUTPUT_BASE_DIR).mkdir(exist_ok=True)

    # Determine tiles or bbox
    tiles = TILES_TO_PROCESS if USE_TILES else None
    bbox = BBOX if USE_BBOX else None

    if not tiles and not bbox:
        print("Error: Must specify either tiles (USE_TILES=True) or bbox (USE_BBOX=True)")
        return

    # Create unified downloader
    downloader = UnifiedGFSCDownloader()

    # Check availability
    if not downloader.wekeo and not downloader.s3:
        print("Error: Neither WEkEO nor S3 downloader could be initialized")
        print("Check your credentials and dependencies")
        return

    # Download all data
    downloader.download_all(
        years=YEARS_TO_PROCESS,
        months=MONTHS_TO_PROCESS,
        tiles=tiles,
        bbox=bbox,
        test_mode=TEST_MODE,
        test_days=TEST_DAYS,
        dry_run=DRY_RUN
    )

    print("\n✓ Download complete!")
    print(f"\nNext steps:")
    print(f"1. Verify downloaded data in: {OUTPUT_BASE_DIR}")
    print(f"2. Process with: python gfsc_snow_probability_processor.py")


if __name__ == "__main__":
    main()