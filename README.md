# GFSC Snow Probability Tools

Verktøy for nedlasting og prosessering av Gap-filled Fractional Snow Cover (GFSC) data fra Copernicus Land Monitoring Service (CLMS) for beregning av snøsannsynlighet.

## Hva er GFSC?

[Gap-filled Fractional Snow Cover](https://land.copernicus.eu/en/products/snow/high-resolution-gap-filled-fractional-snow-cover) er et daglig snødekkeprodukt med 60m oppløsning som kombinerer Sentinel-1 (radar) og Sentinel-2 (optisk) data for robust snødeteksjon med gap-filling.

## Innhold

| Fil | Beskrivelse |
|-----|-------------|
| `gfsc_data_downloader.py` | Last ned GFSC-data fra WEkEO (2017-2024) og S3 (2025+) |
| `gfsc_snow_probability_processor.py` | Beregn snøsannsynlighet fra nedlastede data |

## Hurtigstart

### 1. Installer avhengigheter

```bash
pip install rasterio numpy pandas matplotlib seaborn hda boto3 retry tqdm geopandas shapely pyproj pyogrio
```

### 2. Last ned data

Rediger `gfsc_data_downloader.py` med dine credentials og kjør:

```bash
python gfsc_data_downloader.py
```

### 3. Prosesser data

```bash
python gfsc_snow_probability_processor.py
```

## Dokumentasjon

Se [Gap-filled Fractional Snow Cover](https://land.copernicus.eu/en/products/snow/high-resolution-gap-filled-fractional-snow-cover) for detaljer om GFSC-produktet.

## Credentials

- **WEkEO** (data før 2025-01-20): Registrer deg på [WEkEO](https://www.wekeo.eu/register)
- **S3** (data fra 2025-01-20): Ingen registrering nødvendig

## Lisens

MIT License - se [LICENSE](LICENSE)

Nedlastingsskriptet er basert på [eea/clms-hrsi-api-client-python](https://github.com/eea/clms-hrsi-api-client-python).

