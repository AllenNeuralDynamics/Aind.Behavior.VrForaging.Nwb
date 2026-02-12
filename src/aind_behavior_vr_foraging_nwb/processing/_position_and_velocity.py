import logging
import typing as ty

import contraqctor.contract
import numpy as np
import pandas as pd
from ndx_events import NdxEventsNWBFile
from pydantic import BaseModel
from pynwb import TimeSeries
from pynwb.behavior import Position, SpatialSeries

from .._base import AbstractProcessor
from ._create_processing_module import CreateProcessingModuleProcessor

logger = logging.getLogger(__name__)


class PositionAndVelocityProcessor(AbstractProcessor):
    def __init__(self, dataset: contraqctor.contract.Dataset, *, sampling_rate_hz: ty.Optional[float] = None, **kwargs):
        super().__init__(dataset=dataset, **kwargs)
        self._sampling_rate_hz = sampling_rate_hz

    def process(self, nwb_file: NdxEventsNWBFile) -> NdxEventsNWBFile:
        processing_module = nwb_file.processing.get(CreateProcessingModuleProcessor.module_name())
        if processing_module is None:
            raise ValueError(
                f"Processing module '{CreateProcessingModuleProcessor.module_name()}' not found in NWB file. Please run '{CreateProcessingModuleProcessor.__name__}' processor first to create the processing module."
            )

        position_and_velocity = self._compute_position_and_velocity(
            self.dataset, downsample_to_hz=self._sampling_rate_hz
        )

        position_series = SpatialSeries(  # https://nwb-schema.readthedocs.io/en/latest/format.html#spatialseries
            name="position",
            data=position_and_velocity["position"].values,
            unit="cm",
            timestamps=position_and_velocity.index.values,
        )

        velocity_series = TimeSeries(
            name="velocity",
            data=position_and_velocity["velocity"].values,
            unit="cm/s",
            timestamps=position_and_velocity.index.values,
        )

        processing_module.add(
            Position(spatial_series=position_series)
        )  # https://nwb-schema.readthedocs.io/en/latest/format.html#position
        processing_module.add(velocity_series)

        return nwb_file

    def _compute_position_and_velocity(
        self, dataset: contraqctor.contract.Dataset, *, downsample_to_hz: ty.Optional[float]
    ) -> pd.DataFrame:
        """Computes position and velocity from treadmill encoder data"""
        dataset.at("Behavior").at("InputSchemas").load_all()

        rig_settings = dataset.at("Behavior").at("InputSchemas").at("Rig").load().data
        rig_settings = rig_settings.model_dump() if isinstance(rig_settings, BaseModel) else rig_settings

        try:
            # For backwards compatibility, we need to check for both "calibration" and "output" keys in the rig settings. We should consider removing this in the future.
            calibration = rig_settings["harp_treadmill"]["calibration"]
            assert calibration is not None, (
                "Calibration data is missing for HarpTreadmill. Cannot compute position and velocity."
            )
            calibration = calibration["output"] if "output" in calibration else calibration
            converting_factor = (
                calibration["wheel_diameter"]
                * np.pi
                / calibration["pulses_per_revolution"]
                * (-1 if calibration["invert_direction"] else 1)
            )
        except KeyError as e:
            e.add_note(
                "Missing calibration data for HarpTreadmill in rig settings. Cannot compute position and velocity."
            )
            raise

        ## Parse velocity and position
        treadmill_data = ty.cast(
            pd.DataFrame,
            dataset.at("Behavior").at("HarpTreadmill").load().at("SensorData").load().data,
        )
        encoder = treadmill_data.query("MessageType == 'EVENT'")["Encoder"].copy()

        position = (encoder - encoder.iloc[0]) * converting_factor

        displacement = position.diff().fillna(0)
        velocity = displacement / position.index.to_series().diff().fillna(1)
        df = pd.DataFrame({"position": position, "velocity": velocity})
        if downsample_to_hz is None:
            return df

        df.sort_index(inplace=True)
        df.index = pd.to_timedelta(df.index, unit="s")
        dt = pd.to_timedelta(1.0 / downsample_to_hz, unit="s")
        df = df.resample(dt, label="right", closed="right").mean()
        df.dropna(inplace=True)
        df.index = df.index.total_seconds()  # Convert back to harp time!

        return df
