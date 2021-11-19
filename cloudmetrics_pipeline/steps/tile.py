"""
Functionality to add tiling step to pipeline where an array is split using a
sliding-window approach
"""
import numpy as np
import xarray as xr


def get_sliding_window_view_strided(
    da, window_size, window_stride=None, window_offset=None
):
    if len(da.dims) != 2:
        raise NotImplementedError(da.dims)
    x_dim, y_dim = da.dims
    if window_stride is None:
        window_stride = window_size

    if window_offset == "stride_center":
        if window_stride >= window_size:
            offset = (window_stride - window_size) // 2
        else:
            raise Exception("`stride_center` requires window_stride >= window_size")
    elif window_offset is None:
        offset = 0
    else:
        raise NotImplementedError(window_offset)

    sliding_window_view = np.lib.stride_tricks.sliding_window_view  # noqa

    arr = sliding_window_view(da, window_shape=(window_size, window_size))[
        offset::window_stride, offset::window_stride
    ]

    dims = [
        f"{x_dim}_stride",
        f"{y_dim}_stride",
        f"{x_dim}_window",
        f"{y_dim}_window",
    ]

    def _extract_coord_values(da_coord):
        return sliding_window_view(da_coord, window_shape=(window_size))[
            offset::window_stride
        ][:, 0]

    x_stride_values = _extract_coord_values(da[x_dim])
    y_stride_values = _extract_coord_values(da[y_dim])

    da_windowed = xr.DataArray(
        arr,
        dims=dims,
        coords={f"{x_dim}_stride": x_stride_values, f"{y_dim}_stride": y_stride_values},
    )

    da_windowed.attrs["x_dim"] = f"{x_dim}_stride"
    da_windowed.attrs["y_dim"] = f"{y_dim}_stride"
    return da_windowed


def _plot_strides(da_):
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(
        nrows=da_.x_stride.count().item(),
        ncols=da_.y_stride.count().item(),
        figsize=(6, 6.15),
    )

    for i in da_.x_stride.values[::]:
        for j in da_.y_stride.values[::]:
            ax = axes[i.item(), j.item()]
            da_.sel(x_stride=da_.x_stride.count().item() - 1 - i, y_stride=j).plot(
                ax=ax, add_colorbar=False
            )

            ax.axis("off")

    [ax.set_aspect(1.0) for ax in axes.flatten()]
    # fig.tight_layout()
    fig.subplots_adjust(wspace=0.1, hspace=0.1)
