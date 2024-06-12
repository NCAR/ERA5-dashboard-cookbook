from dask_fn import get_local_cluster
from distributed import Client
import intake
import panel as pn
from plot_fn import plot_ds
import xarray as xr

catalog_url = 'https://data.rda.ucar.edu/pythia_era5_24/pythia_intake_catalogs/era5_catalog.json'

zarr_file = 'https://data.rda.ucar.edu/pythia_era5_24/annual_means/temp_2m_annual_1940_2023.zarr'

era5_ds = xr.open_dataset(zarr_file, engine= 'zarr')

client = Client(get_local_cluster())

era5_cat = intake.open_esm_datastore(catalog_url)

era5_df = era5_cat.df

# Select the variable to plot
var_select_options = era5_df['variable'].unique()

var_select = pn.widgets.Select(
    name = 'Variable',
    value = var_select_options.get(list(var_select_options.keys())[0]),
    options = var_select_options
)

# Select the year
year_slider = pn.widgets.IntSlider(
    name = 'Year',
    start = t2m_colorado_baseline.value.time.min().item().year,
    end = t2m_colorado_current.value.time.max().item().year,
)

# Select the color map to use
cmap_select = pn.widgets.Select(
    name = 'Colormap',
    options = ['viridis', 'inferno', 'inferno_r', 'kb', 'coolwarm', 'coolwarm_r', 'Blues', 'Blues_r']
)

temp_cat = era5_cat.search(variable='VAR_2T',frequency = 'hourly')

pn.Row(
    pn.Column(
        var_select,
        year_slider,
        cmap_select,
    ),
    pn.panel(
        pn.bind(
            plot_ds, 
            da=var_select,
            year=year_slider,
            cmap=cmap_select
        )
    )
).servable()