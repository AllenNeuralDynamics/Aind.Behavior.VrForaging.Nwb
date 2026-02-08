import logging
from typing import Optional

import contraqctor.contract as data_contract
import pynwb
from ndx_events import NdxEventsNWBFile

from .._base import AbstractProcessor
from . import helper

logger = logging.getLogger(__name__)


class AcquisitionProcessor(AbstractProcessor):
    def __init__(self, dataset: data_contract.Dataset) -> None:
        self._dataset = dataset
        self._nwb_file: Optional[NdxEventsNWBFile] = None

    def with_nwb_file(self, nwb_file: NdxEventsNWBFile) -> "AcquisitionProcessor":
        self._nwb_file = nwb_file
        return self

    def process(self) -> NdxEventsNWBFile:
        if self._nwb_file is None:
            raise ValueError(
                "NWB file must be set before processing acquisition data. Use with_nwb_file(...) to set it."
            )
        _ = self._dataset.load_all(strict=False)

        for stream in self._dataset.iter_all():
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
                    self._nwb_file.add_acquisition(dynamic_table)
                elif isinstance(stream, (data_contract.json.SoftwareEvents)):
                    data = helper.clean_dataframe_for_nwb(stream.data.reset_index())
                    dynamic_table = pynwb.core.DynamicTable.from_dataframe(
                        name=name, table_description=stream.description, df=data
                    )
                    self._nwb_file.add_acquisition(dynamic_table)

                elif isinstance(stream, data_contract.json.PydanticModel):
                    self._nwb_file.add_acquisition(
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
        return self._nwb_file
