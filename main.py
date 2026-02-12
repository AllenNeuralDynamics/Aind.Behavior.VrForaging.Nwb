from pathlib import Path

from aind_behavior_vr_foraging_nwb.acquisition import AcquisitionProcessor
from aind_behavior_vr_foraging_nwb.nwb_file import NwbSession
from aind_behavior_vr_foraging_nwb.processing import (
    CreateProcessingModuleProcessor,
    PositionAndVelocityProcessor,
    TrialTableProcessor,
)

dataset_path = Path(r"C:\Data\single-site-dataset\behavior_808728_2025-12-15_21-19-25")
session = NwbSession(root_path=dataset_path)
session.run(
    AcquisitionProcessor(session.dataset),
    CreateProcessingModuleProcessor(session.dataset),
    PositionAndVelocityProcessor(session.dataset, sampling_rate_hz=100.0),
    TrialTableProcessor(session.dataset),
)

a = session.nwb_file.trials.to_dataframe()
rewarded_sites = a[a["site_label"] == "RewardSite"]
for patch_id in rewarded_sites["patch_label"].unique():
    patch_data = rewarded_sites[rewarded_sites["patch_label"] == patch_id]
    p_choice = patch_data["has_choice"].mean()
    p_reward = patch_data["has_reward"].sum() / len(patch_data)
    print(f"Patch {patch_id}: P(choice)={p_choice:.2f}, P(reward|choice)={p_reward:.2f}")

session.write_nwb_zarr(".tmp/output.nwb.zarr")
