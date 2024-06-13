from app import year_slider
import cftime
from holoviews.operation.datashader import rasterize

def plot_ds(da, year, cmap):
    year_slider.start = da.time.min().item().year
    year_slider.end = da.time.max().item().year
    
    if 'long_name' in da.attrs:
        var_long_name = da.attrs['long_name']
    else:
        var_long_name = var
    
    if 'units' in da.attrs:
        clabel = f'{var_long_name} ({da.attrs["units"]})'
    else:
        clabel = f'{var_long_name} (undefined units)'
    
    return rasterize(
        da.sel(time=cftime.datetime(year, 1, 1, calendar='noleap'), method='nearest').hvplot(
            x='lon',
            y='lat',
            geo=True,
            coastline=True,
            cmap=cmap,
            clabel=clabel
        ).opts(
            title=f"Average annual {var_long_name} on {year}",
            frame_width=400
        ))