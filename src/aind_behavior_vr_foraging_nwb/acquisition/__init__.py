import dataclasses
import logging
from pathlib import Path
from typing import Optional

import aind_behavior_vr_foraging.data_contract
import contraqctor.contract as data_contract
import pynwb
import semver
from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.core.data_description import DataDescription
from aind_data_schema.core.instrument import Instrument
from aind_data_schema.core.subject import Subject
from aind_nwb_utils.utils import get_subject_nwb_object
from hdmf_zarr import NWBZarrIO
from ndx_events import NdxEventsNWBFile

from .._base import AbstractProcessor
from . import helper

logger = logging.getLogger(__name__)


@dataclasses.dataclass(kw_only=True)
class _AindDataSchemaJson:
    acquisition: Acquisition
    instrument: Instrument
    subject: Subject
    data_description: DataDescription

    @classmethod
    def from_root_path(cls, root_path: Path) -> "_AindDataSchemaJson":
        acquisition_json_path = tuple(root_path.glob("*acquisition*.json"))
        data_description_json_path = tuple(root_path.glob("*data_description*.json"))
        subject_json_path = tuple(root_path.glob("*subject*.json"))
        instrument_json_path = tuple(root_path.glob("*instrument*.json"))

        assert len(acquisition_json_path) == 1, (
            f"Expected exactly 1 acquisition.json, found {len(acquisition_json_path)}"
        )
        assert len(instrument_json_path) == 1, f"Expected exactly 1 instrument.json, found {len(instrument_json_path)}"
        assert len(subject_json_path) == 1, f"Expected exactly 1 subject.json, found {len(subject_json_path)}"
        assert len(data_description_json_path) == 1, (
            f"Expected exactly 1 data_description.json, found {len(data_description_json_path)}"
        )

        return cls(
            acquisition=Acquisition.model_validate_json(acquisition_json_path[0].read_text()),
            instrument=Instrument.model_validate_json(instrument_json_path[0].read_text()),
            subject=Subject.model_validate_json(subject_json_path[0].read_text()),
            data_description=DataDescription.model_validate_json(data_description_json_path[0].read_text()),
        )


class AcquisitionProcessor(AbstractProcessor):
    def __init__(self, root_path: Path, *, dataset: Optional[data_contract.Dataset] = None) -> None:
        self.root_path = root_path
        self.dataset = dataset if dataset else aind_behavior_vr_foraging.data_contract.dataset(root_path)
        self.aind_data_schema = self._get_aind_data_schema_json()

    def _get_aind_data_schema_json(self) -> _AindDataSchemaJson:
        jsons = _AindDataSchemaJson.from_root_path(self.root_path)
        logger.debug("Found primary data %s", jsons.data_description.name)
        return jsons

    @property
    def dataset_version(self) -> semver.Version:
        return self._parse_version(self.dataset.version)

    @property
    def parser_version(self) -> semver.Version:
        return semver.Version.parse(aind_behavior_vr_foraging.__semver__)

    @staticmethod
    def _parse_version(value: str | semver.Version) -> semver.Version:
        if isinstance(value, semver.Version):
            return value
        return semver.Version.parse(value)

    def process(self) -> NdxEventsNWBFile:
        _ = self.dataset.load_all(strict=False)

        # using this ndx object for events table
        nwb_file = NdxEventsNWBFile(
            session_id=self.aind_data_schema.data_description.name,
            session_description=f"Dataset version: {self.dataset_version}",
            session_start_time=self.aind_data_schema.acquisition.acquisition_start_time,
            identifier=self.aind_data_schema.data_description.subject_id,
            subject=get_subject_nwb_object(
                self.aind_data_schema.data_description.model_dump(), self.aind_data_schema.subject.model_dump()
            ),
        )
        for stream in self.dataset.iter_all():
            if stream.is_collection:  # only process leaf nodes into nwb
                err = stream.collect_errors()
                if err:
                    logger.warning(f"Collection stream {stream.name} has errors: {err}")
                    if self.raise_on_error:
                        raise ValueError(f"Collection stream {stream.name} has errors: {err}")
                continue

            name = stream.resolved_name.replace("::", ".")
            try:
                if stream.has_error:
                    logger.warning(f"Stream {stream.name} has error: {stream.collect_errors()}")
                    if self.raise_on_error:
                        raise ValueError(f"Stream {stream.name} has error: {stream.collect_errors()}")
                if isinstance(stream, (data_contract.harp.HarpRegister, data_contract.csv.Csv)):
                    dynamic_table = pynwb.core.DynamicTable.from_dataframe(
                        name=name,
                        table_description=stream.description,
                        df=stream.data.reset_index(),
                    )
                    nwb_file.add_acquisition(dynamic_table)
                elif isinstance(stream, (data_contract.json.SoftwareEvents)):
                    data = helper.clean_dataframe_for_nwb(stream.data.reset_index())
                    dynamic_table = pynwb.core.DynamicTable.from_dataframe(
                        name=name, table_description=stream.description, df=data
                    )
                    nwb_file.add_acquisition(dynamic_table)

                elif isinstance(stream, data_contract.json.PydanticModel):
                    nwb_file.add_acquisition(
                        pynwb.core.DynamicTable(
                            name=name,
                            description=stream.data.model_dump_json(),
                        )
                    )
                else:
                    raise ValueError(f"Stream {stream.name} has unsupported type {type(stream)}, skipping.")
            except Exception as e:
                if self.raise_on_error:
                    logger.error(f"Error processing stream {stream.name}: {e}")
                    raise
                else:
                    logger.warning(f"Error processing stream {stream.name}: {e}")
        return nwb_file

    @staticmethod
    def write_nwb_zarr(nwb_file: NdxEventsNWBFile, output: Path) -> None:
        with NWBZarrIO((output).as_posix(), "w") as io:
            io.write(nwb_file)
        logger.info(f"NWB zarr successfully written to path {output}")
