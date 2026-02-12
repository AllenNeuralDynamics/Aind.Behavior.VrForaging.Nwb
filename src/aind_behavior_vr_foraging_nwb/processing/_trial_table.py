import logging
import typing as t

import contraqctor
import numpy as np
import pandas as pd
import semver
from contraqctor.contract.json import PydanticModel
from ndx_events import NdxEventsNWBFile
from pydantic import BaseModel

from .._base import AbstractProcessor
from ..models import Site
from .helper import slice_by_index

logger = logging.getLogger(__name__)


class DatasetProcessorError(Exception):
    pass


class TrialTableProcessor(AbstractProcessor):
    def __init__(self, dataset: contraqctor.contract.Dataset, *, raise_on_error: bool = False) -> None:
        super().__init__(dataset, raise_on_error=raise_on_error)

        if self.dataset_version != self.parser_version:
            logger.warning(
                "Dataset version %s does not match parser version %s", self.dataset_version, self.parser_version
            )

    @staticmethod
    def _parse_speaker_choice_feedback(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
        speaker_choice = dataset.at("Behavior").at("HarpBehavior").load().at("PwmStart").load().data
        speaker_choice = speaker_choice[(speaker_choice["MessageType"] == "WRITE") & (speaker_choice["PwmDO2"])]
        return speaker_choice

    @staticmethod
    def _parse_water_delivery(dataset: contraqctor.contract.Dataset) -> pd.Series:
        water_delivery = dataset.at("Behavior").at("HarpBehavior").load().at("OutputSet").load().data
        water_delivery = water_delivery[(water_delivery["MessageType"] == "WRITE") & (water_delivery["SupplyPort0"])][
            "SupplyPort0"
        ]
        return water_delivery

    @staticmethod
    def _parse_odor_onset(dataset: contraqctor.contract.Dataset) -> pd.Series:
        odor_onset = dataset.at("Behavior").at("HarpOlfactometer").load().at("EndValveState").load().data
        odor_onset = odor_onset[odor_onset["MessageType"] == "WRITE"]["EndValve0"]
        odor_onset = odor_onset[(odor_onset) & (~odor_onset.shift(1, fill_value=False))]
        return odor_onset

    @staticmethod
    def _parse_continuous_patch_state(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
        patches_state = dataset.at("Behavior").at("SoftwareEvents").at("PatchState").load().data
        expanded = pd.json_normalize(patches_state["data"])
        expanded.index = patches_state.index
        patches_state = patches_state.join(expanded)
        return patches_state

    @staticmethod
    def _parse_patch_state_at_reward(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
        patches_state_at_reward = dataset.at("Behavior").at("SoftwareEvents").at("PatchStateAtReward").load().data
        expanded = pd.json_normalize(patches_state_at_reward["data"])
        expanded.index = patches_state_at_reward.index
        patches_state_at_reward = patches_state_at_reward.join(expanded)
        return patches_state_at_reward

    @staticmethod
    def _parse_wait_reward_outcome(dataset: contraqctor.contract.Dataset) -> pd.Series:
        try:
            return dataset.at("Behavior").at("SoftwareEvents").at("WaitRewardOutcome").load().data
        except FileNotFoundError:
            return pd.Series(dtype=bool)

    @staticmethod
    def _parse_reward_metadata(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
        reward_metadata = dataset.at("Behavior").at("SoftwareEvents").at("GiveReward").load().data
        return reward_metadata

    @staticmethod
    def _as_dict(d: contraqctor.contract.DataStream | PydanticModel | BaseModel | dict) -> dict:
        if isinstance(d, (PydanticModel, contraqctor.contract.DataStream)):
            d = d.data
        if isinstance(d, dict):
            return d
        if isinstance(d, BaseModel):
            return d.model_dump()
        else:
            raise TypeError(f"Cannot convert type {type(d)} to dict")

    @staticmethod
    def _parse_friction(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
        d = dataset.at("Behavior").at("HarpTreadmill").at("BrakeCurrentSetPoint").load().data
        return d.loc[d["MessageType"] == "WRITE", "BrakeCurrentSetPoint"]

    def _get_olfactometer_channel_count(self, dataset: contraqctor.contract.Dataset) -> int:
        if self.dataset_version < semver.Version.parse("0.7.0"):
            return 3  # The channel 3 is always used as carrier, therefor only 3 odor channels are available.
        else:
            raise NotImplementedError("Olfactometer channel count parsing not implemented for rig versions < 0.7.0")

    def _process_odor_concentration(self, odor_specification: BaseModel | dict | None, n_channels: int) -> list[float]:
        concentration = [0.0] * n_channels
        if odor_specification is None:
            return concentration
        if isinstance(odor_specification, BaseModel):
            odor_specification = odor_specification.model_dump()

        match v := self.dataset_version:
            case _ if v < semver.Version.parse("0.7.0"):
                index = odor_specification.get("index")
                if not isinstance(index, int):
                    raise TypeError("odor_specification.index must be an int")
                concentration[index] = odor_specification.get("concentration", 0.0)
            case _:
                raise NotImplementedError("OdorSpecification processing not implemented for rig versions >= 0.7.0")
        return concentration

    def process(self, nwb_file: NdxEventsNWBFile) -> NdxEventsNWBFile:
        sites = self.process_to_sites()
        for field_name, field in Site.model_fields.items():
            if field_name in ["start_time", "stop_time"]:
                continue
            nwb_file.add_trial_column(name=field_name, description=field.description)

        for site in sites:
            trial_data = site.model_dump()
            # Replace None with np.nan
            trial_data = {k: (np.nan if v is None else v) for k, v in trial_data.items()}
            nwb_file.add_trial(**trial_data)
        return nwb_file

    def process_to_sites(self) -> list[Site]:
        """
        Processes sites, patches, and blocks from the dataset and merges them.
        Returns a DataFrame with merged information.
        """
        dataset = self.dataset
        odor_sites = t.cast(pd.DataFrame, dataset.at("Behavior").at("SoftwareEvents").at("ActiveSite").load().data)
        patches = t.cast(pd.DataFrame, dataset.at("Behavior").at("SoftwareEvents").at("ActivePatch").load().data)
        patches["patch_count"] = range(len(patches))
        blocks = t.cast(pd.DataFrame, dataset.at("Behavior").at("SoftwareEvents").at("Block").load().data)
        blocks["block_count"] = range(len(blocks))

        # Merge nearest patch (backward in time)
        merged = pd.merge_asof(
            odor_sites.sort_index(),
            patches[["data", "patch_count"]].rename(columns={"data": "patch_data"}).sort_index(),
            left_index=True,
            right_index=True,
            direction="backward",
            suffixes=("", "_patch"),
        )
        merged["patch_index"] = merged["patch_data"].apply(lambda d: d["state_index"])

        # Merge nearest block (backward in time)
        merged = pd.merge_asof(
            merged.sort_index(),
            blocks[["block_count"]].sort_index(),
            left_index=True,
            right_index=True,
            direction="backward",
        )

        choice_feedback = self._parse_speaker_choice_feedback(dataset)
        water_delivery = self._parse_water_delivery(dataset)
        reward_metadata = self._parse_reward_metadata(dataset)
        odor_onset = self._parse_odor_onset(dataset)
        patch_state_at_reward = self._parse_patch_state_at_reward(dataset)
        friction = self._parse_friction(dataset)
        olfactometer_channel_count = self._get_olfactometer_channel_count(dataset)
        wait_reward_outcome = self._parse_wait_reward_outcome(dataset)

        # Mutable state variables
        current_friction = 0  # Keeps track of the last known friction. Sites with null friction will not update this.
        current_block_idx = 0
        current_patch_idx = 0
        current_patch_in_block_idx = 0  # Resets when block changes
        current_site_in_patch_idx = 0  # Resets when patch changes
        current_site_in_block_idx = 0  # Resets when block changes
        unique_site_labels = merged["data"].apply(lambda d: d["label"]).unique().tolist()
        site_by_type_in_patch_counter = dict.fromkeys(
            unique_site_labels, 0
        )  # initialize to 0, which means we subtract 1 later for 0-based indexing

        ##

        sites: list[Site] = []
        # We reject the last site because it may not have completed and would require custom logic to handle
        for i in range(len(merged) - 1):
            # We generally assume that all relevant events happen within the software-event derived timestamp intervals
            # Note this may not always be true depending on system jitter, but it is generally a safe assumption.
            # If you find edge cases where this is not true, submit an issue so we can investigate and improve the parser.

            this_timestamp = merged.index[i]
            next_timestamp = merged.index[i + 1]

            this_site = merged.iloc[i]["data"]
            this_patch = merged.iloc[i]["patch_data"]

            site_choice_feedback = slice_by_index(choice_feedback, this_timestamp, next_timestamp)
            assert len(site_choice_feedback) <= 1, "Multiple speaker choices in site interval"

            site_water_delivery = slice_by_index(water_delivery, this_timestamp, next_timestamp)
            assert len(site_water_delivery) <= 1, "Multiple water deliveries in site interval"

            site_odor_onset = slice_by_index(odor_onset, this_timestamp, next_timestamp)
        
            this_friction = slice_by_index(friction, this_timestamp, next_timestamp)
            if not this_friction.empty:
                current_friction = this_friction.values[-1]

            site_patch_state_at_reward = slice_by_index(patch_state_at_reward, this_timestamp, next_timestamp)
            site_patch_state_at_reward = site_patch_state_at_reward[
                site_patch_state_at_reward["PatchId"] == merged.iloc[i]["patch_index"]
            ]
            assert len(site_patch_state_at_reward) <= 1, "Multiple patch states at reward in site interval"

            ##
            this_block_idx = merged.iloc[i]["block_count"]
            this_patch_idx = merged.iloc[i]["patch_count"]

            # We always increment these eagerly
            current_site_in_patch_idx += 1
            current_site_in_block_idx += 1

            # If the patch changed, we reset the site_in_patch counter and increment the patch_in_block counter
            if this_patch_idx != current_patch_idx:
                current_patch_idx = this_patch_idx
                current_site_in_patch_idx = 0
                current_patch_in_block_idx += 1
                site_by_type_in_patch_counter = dict.fromkeys(unique_site_labels, 0)

            # If the blocked changed, we reset both the patch_in_block and site_in_block counters
            # We dont need to re-reset current_patch_idx because patches are unique across blocks
            if this_block_idx != current_block_idx:
                current_block_idx = this_block_idx
                current_patch_in_block_idx = 0
                current_site_in_block_idx = 0

            site_by_type_in_patch_counter[this_site["label"]] += 1

            choice_time = site_choice_feedback.index[0] if not site_choice_feedback.empty else np.nan

            if site_odor_onset.empty and this_site["odor_specification"] is not None:
                # Sometimes the timestamp for the odor onset arrives slightly before the site. We should investigate
                # but for now we just log a warning and use the site onset instead after checking if this is the issue
                odor_onset_before_site = odor_onset[
                    (odor_onset.index < this_timestamp) & (odor_onset.index >= this_timestamp - 0.002)
                ]  # we use a 2ms conservative window
                if odor_onset_before_site.empty:
                    if self.raise_on_error:
                        raise DatasetProcessorError("No odor onset found in site interval")
                    else:
                        logger.warning("No odor onset found in site interval")
                        odor_onset_time = np.nan
                else:
                    logger.warning("Odor onset found slightly (<2ms) before site interval, using site onset instead")
                    odor_onset_time = this_timestamp
            else:
                # we always take the first odor onset in case animal goes in and out
                odor_onset_time = site_odor_onset.index[0] if not site_odor_onset.empty else np.nan

            reward_metadata_sliced = slice_by_index(reward_metadata, this_timestamp, next_timestamp)
            if reward_metadata_sliced.empty or reward_metadata_sliced["data"].fillna(0).eq(0).all():
                # Note: for None or 0 reward metadata there wont be a hardware water delivery event
                # However, if the experimenter manually triggered a reward around this time, we should not count that
                # as a reward for this site either, so we make an explicit decision to set reward_onset_time to nan
                reward_onset_time = np.nan
            else:
                if len(site_water_delivery) == 0:
                    if self.raise_on_error:
                        raise DatasetProcessorError(
                            "Valid reward metadata found but no water delivery in site interval"
                        )
                    else:
                        logger.warning("Valid reward metadata found but no water delivery in site interval")
                        reward_onset_time = np.nan
                elif len(reward_metadata_sliced) > 1:
                    logger.warning("Multiple reward metadata entries in site interval...Using first one")
                    reward_onset_time = site_water_delivery.index[0]
                else:
                    reward_onset_time = site_water_delivery.index[0] if not site_water_delivery.empty else np.nan

            wait_reward_outcome_sliced = slice_by_index(wait_reward_outcome, this_timestamp, next_timestamp)
            has_waited_reward_delay = (
                wait_reward_outcome_sliced.iloc[0]["data"]["IsSuccessfulWait"]
                if not wait_reward_outcome_sliced.empty
                else None
            )

            site = Site(
                start_time=this_timestamp,
                stop_time=next_timestamp,
                start_position=this_site["start_position"],
                length=this_site["length"],
                site_label=str(this_site["label"]),
                friction=current_friction,
                patch_label=str(this_patch["label"]),
                odor_concentration=self._process_odor_concentration(
                    this_patch["odor_specification"], olfactometer_channel_count
                ),
                patch_index=current_patch_idx,
                patch_in_block_index=current_patch_in_block_idx,
                site_index=i,
                site_in_patch_index=current_site_in_patch_idx,
                site_in_block_index=current_site_in_block_idx,
                site_by_type_in_patch_index=site_by_type_in_patch_counter[this_site["label"]] - 1,  # zero indexed
                odor_onset_time=odor_onset_time,
                reward_onset_time=reward_onset_time,
                reward_amount=np.nan
                if site_patch_state_at_reward.empty
                else site_patch_state_at_reward.iloc[0]["Amount"],
                reward_probability=np.nan
                if site_patch_state_at_reward.empty
                else site_patch_state_at_reward.iloc[0]["Probability"],
                reward_available=np.nan
                if site_patch_state_at_reward.empty
                else site_patch_state_at_reward.iloc[0]["Available"],
                has_reward=np.isnan(reward_onset_time) == False,  # noqa: E712
                choice_cue_time=choice_time,
                has_choice=not site_choice_feedback.empty,
                reward_delay_duration=reward_onset_time - odor_onset_time,
                has_waited_reward_delay=has_waited_reward_delay,
                block_index=this_block_idx,
            )
            sites.append(site)
        return sites
