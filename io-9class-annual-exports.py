import numpy as np
import pandas as pd
import planetary_computer
import rasterio
import stackstac
import pystac_client
import json
import geopandas as gpd
import os
import rioxarray


def get_bbox(path):
    """Given path to geojson, return AOI bounding box."""

    # Load the geojson.
    aoi_file = gpd.read_file(path)

    # Calculate the bounding box [minx, miny, maxx, maxy].
    bounds = aoi_file.total_bounds 
    
    return(bounds)


def check_item_crs(items):
    """Check the CRS is the same for all items returned."""

    # Get the EPSG code for the first item.
    epsg_code = items[0].properties.get("proj:code")

    # Check that all other items have the same EPSG code.
    for item in items:
        assert(item.properties.get("proj:code") == epsg_code)
    print(f"EPSG codes of all items is: {epsg_code}")
    return epsg_code


def search_catalog(catalog, collection, bbox):
    """Search the MPC catalog for items of a collection intersecting the AOI bbox."""

    search = catalog.search(collections=[collection], bbox=bbox)
    items = search.item_collection()
    print(f"Returned {len(items)} items intersecting the bbox.")

    return items

def stack_items(items, epsg_code, bbox):
    """Stack items returned for an AOI into an xarray DataArray."""

    # Extract just the EPSG integer from the EPSG code.
    epsg_int = int(epsg_code.split(":")[-1])

    # Stack the items into an xarray DataArray.
    stack = (
        stackstac.stack(
            items,
            dtype="int64", # Impact Observatory uses uint8, but issues with fill_value of 0 unless using int64 here.
            rescale=False, # Whether to rescale using scale/offset in band metadata - default is True, but we don't need this.
            fill_value=0, # Default is nan, but Impact Observatory uses 0 (nan causes issues with uint8).
            epsg=epsg_int,
            bounds_latlon=bbox, # We clip later as well, but setting this here is much quicker!
            snap_bounds=True, # Default is true - snaps pixel edges to the bounds specified.
            #resampling="nearest", # Default is nearest, which works for land cover data.
        )
        .assign_coords(
            time=pd.to_datetime([item.properties["start_datetime"] for item in items])
            .tz_convert(None)
            .to_numpy()
        )
        .sortby("time")
    )
    return stack

def export_items_from_stack(stack, output_dir, aoi_path):
    # Loop through each time slice and save as a GeoTIFF.
    for i, time_val in enumerate(stack.time.values):
        # Extract a single time slice
        single_item = stack.sel(time=time_val)

        # Reproject AOI to match the raster's CRS.
        aoi_file = gpd.read_file(aoi_path)
        aoi_reprojected = aoi_file.to_crs(single_item.rio.crs)
        aoi_geom = [aoi_reprojected.geometry.iloc[0]]

        # Clip the raster to the AOI boundary with all_touched=True.
        clipped = single_item.rio.clip(aoi_geom, 
                                       all_touched=True)

        # Avoid any possible NaNs in the final raster for exporting with uint8.
        clipped = clipped.fillna(0)

        # Set nodata value to 0 to appear in raster metadata.
        clipped.rio.write_nodata(0, inplace=True)
        
        # Format date for filename (if using time in the filename).
        date_str = pd.to_datetime(time_val).strftime('%Y%m%d')
        
        # Create output filename.
        output_file = os.path.join(output_dir, f"io_land_cover_{date_str}_{i}.tif")

        # Save to GeoTIFF.
        clipped.rio.to_raster(
            output_file,
            driver="GTiff",
            dtype="uint8")
        
        print(f"Saved {output_file}")
    
    return



def export_mpc_data(catalog, collection, aoi_path):

    bbox = get_bbox(aoi_path)
    items = search_catalog(catalog, collection, bbox)
    crs = check_item_crs(items)
    stack = stack_items(items, crs, bbox)
    export_items_from_stack(stack, output_dir, aoi_path)

    return


if __name__ == "__main__":

    # Set up access to Microsoft Planetary Computer Catalog 
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )

    # Impact Observatory 9-class Collection
    collection = "io-lulc-annual-v02"

    # Set AOI path and output directory depending on the AOI. 
    # aoi_path = "/Users/sabinenix/Documents/Data/AOIs/Tensas_River_Basin_aoi.geojson"
    # output_dir = "/Users/sabinenix/Documents/Data/ImpactObservatory/9-class/TensasRiverBasinProject"
    aoi_path = "/Users/sabinenix/Documents/Data/AOIs/Kakadu_National_Park.geojson"
    output_dir = "/Users/sabinenix/Documents/Data/ImpactObservatory/9-class/KakaduNationalPark"
    
    # Check the output directory exists, make it if it doesn't exist.
    os.makedirs(output_dir, exist_ok=True)

    # Export the data
    export_mpc_data(catalog, collection, aoi_path)


    


