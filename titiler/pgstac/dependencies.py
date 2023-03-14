"""titiler-pgstac dependencies."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Sequence, Tuple

import json
import numpy
import pystac
import matplotlib
from cachetools import TTLCache, cached
from cachetools.keys import hashkey
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from titiler.core.dependencies import DefaultDependency
from titiler.pgstac import model
from titiler.pgstac.settings import CacheSettings
from rio_tiler.colormap import cmap, parse_color
from rio_tiler.errors import MissingAssets, MissingBands
from rio_tiler.types import ColorMapType

from fastapi import HTTPException, Path, Query

from starlette.requests import Request

cache_config = CacheSettings()

ColorMapName = Enum(  # type: ignore
    "ColorMapName", [(a, a) for a in sorted(cmap.list())]
)

def PathParams(searchid: str = Path(..., description="Search Id")) -> str:
    """SearcId"""
    return searchid


def SearchParams(
    body: model.RegisterMosaic,
) -> Tuple[model.PgSTACSearch, model.Metadata]:
    """Search parameters."""
    search = body.dict(
        exclude_none=True,
        exclude={"metadata"},
        by_alias=True,
    )
    return model.PgSTACSearch(**search), body.metadata

@dataclass(init=False)	
class BackendParams(DefaultDependency):	
    """backend parameters."""	
    pool: ConnectionPool = field(init=False)	
    def __init__(self, request: Request):	
        """Initialize BackendParams	
        Note: Because we don't want `pool` to appear in the documentation we use a dataclass with a custom `__init__` method.	
        FastAPI will use the `__init__` method but will exclude Request in the documentation making `pool` an invisible dependency.	
        """	
        self.pool = request.app.state.dbpool

@dataclass
class PgSTACParams(DefaultDependency):
    """PgSTAC parameters."""

    scan_limit: Optional[int] = Query(
        None,
        description="Return as soon as we scan N items (defaults to 10000 in PgSTAC).",
    )
    items_limit: Optional[int] = Query(
        None,
        description="Return as soon as we have N items per geometry (defaults to 100 in PgSTAC).",
    )
    time_limit: Optional[int] = Query(
        None,
        description="Return after N seconds to avoid long requests (defaults to 5 in PgSTAC).",
    )
    exitwhenfull: Optional[bool] = Query(
        None,
        description="Return as soon as the geometry is fully covered (defaults to True in PgSTAC).",
    )
    skipcovered: Optional[bool] = Query(
        None,
        description="Skip any items that would show up completely under the previous items (defaults to True in PgSTAC).",
    )


@cached(
    TTLCache(maxsize=cache_config.maxsize, ttl=cache_config.ttl),
    key=lambda pool, collection, item: hashkey(collection, item),
)
def get_stac_item(pool: ConnectionPool, collection: str, item: str) -> pystac.Item:
    """Get STAC Item from PGStac."""
    search = model.PgSTACSearch(ids=[item], collections=[collection])
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                ("SELECT * FROM pgstac.search(%s) LIMIT 1;"),
                (search.json(by_alias=True, exclude_none=True),),
            )

            resp = cursor.fetchone()["search"]
            if not resp or "features" not in resp or len(resp["features"]) != 1:
                raise HTTPException(
                    status_code=404,
                    detail=f"No item '{item}' found in '{collection}' collection",
                )

            return pystac.Item.from_dict(resp["features"][0])


def ItemPathParams(
    request: Request,
    collection_id: str = Path(..., description="STAC Collection ID"),
    item_id: str = Path(..., description="STAC Item ID"),
) -> pystac.Item:
    """STAC Item dependency."""

    return get_stac_item(request.app.state.dbpool, collection_id, item_id)


class ColorMapType(str, Enum):
    """Colormap types."""

    explicit = "explicit"
    linear = "linear"


def ColorMapParams(
    colormap_name: ColorMapName = Query(None, description="Colormap name"),
    colormap: str = Query(None, description="JSON encoded custom Colormap"),
    colormap_type: ColorMapType = Query(ColorMapType.explicit, description="User input colormap type."),
) -> Optional[Dict]:
    """Colormap Dependency."""
    if colormap_name:
        return cmap.get(colormap_name.value)

    if colormap:
        try:
            cm = json.loads(
                colormap,
                object_hook=lambda x: {int(k): parse_color(v) for k, v in x.items()},
            )
            # Make sure to match colormap type
            if isinstance(cm, Sequence):
                cm = [(tuple(inter), parse_color(v)) for (inter, v) in cm]

        except json.JSONDecodeError:
            raise HTTPException(
                status_code=400, detail="Could not parse the colormap value."
            )

        if colormap_type == ColorMapType.linear:
            # input colormap has to start from 0 to 255 ?
            cm = matplotlib.colors.LinearSegmentedColormap.from_list(
                'custom',
                [
                    (k / 255, matplotlib.colors.to_hex([v / 255 for v in rgba], keep_alpha=True))
                    for (k, rgba) in cm.items()
                ],
                256,
            )
            x = numpy.linspace(0, 1, 256)
            cmap_vals = cm(x)[:, :]
            cmap_uint8 = (cmap_vals * 255).astype('uint8')
            cm = {idx: value.tolist() for idx, value in enumerate(cmap_uint8)}

        return cm

    return None


def ProjectionParams(dst_crs: str = Query(default="EPSG:4326", description="Destination CRS")):
    # TODO: validate?
    return dst_crs