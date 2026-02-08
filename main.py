from pathlib import Path

from aind_behavior_vr_foraging_nwb.acquisition import AcquisitionProcessor
from aind_behavior_vr_foraging_nwb.nwb_file import NwbFileProcessor
from aind_behavior_vr_foraging_nwb.processing import TrialTableProcessor

dataset_path = Path(r"\\allen\aind\stage\vr-foraging\data\828424\828424_2026-01-31T001737Z")
nwb_file_processor = NwbFileProcessor(root_path=dataset_path)
processed_acquisitions = (
    AcquisitionProcessor(nwb_file_processor.dataset).with_nwb_file(nwb_file=nwb_file_processor._nwb_file).process()
)
processed_sites = (
    TrialTableProcessor(dataset=nwb_file_processor.dataset)
    .with_nwb_file(nwb_file=nwb_file_processor._nwb_file)
    .process()
)


a = nwb_file_processor._nwb_file.trials.to_dataframe()
rewarded_sites = a[a["site_label"] == "RewardSite"]
for patch_id in rewarded_sites["patch_label"].unique():
    patch_data = rewarded_sites[rewarded_sites["patch_label"] == patch_id]
    p_choice = patch_data["has_choice"].mean()
    p_reward = patch_data["has_reward"].sum() / len(patch_data)
    print(f"Patch {patch_id}: P(choice)={p_choice:.2f}, P(reward|choice)={p_reward:.2f}")

nwb_file_processor.write_nwb_zarr("output.nwb.zarr")
